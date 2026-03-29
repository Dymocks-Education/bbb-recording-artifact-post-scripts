#!/usr/bin/ruby
# encoding: UTF-8

#
# BigBlueButton open source conferencing system - http://www.bigbluebutton.org/
#
# Copyright (c) 2012 BigBlueButton Inc. and by respective authors (see below).
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License as published by the Free
# Software Foundation; either version 3.0 of the License, or (at your option)
# any later version.
#
# BigBlueButton is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License along
# with BigBlueButton; if not, see <http://www.gnu.org/licenses/>.
#

#
# Standalone events.xml fallback processor for recording artifact generation.
#
# This script reconstructs presentation metadata, annotations, page dimensions,
# and user lists from the events.xml event trail and raw archive files. It is
# designed to run externally (e.g., in a container or Lambda) against the raw
# file package produced by post_publish_recording_artifacts.rb when the Phase 1
# Postgres snapshot is unavailable.
#
# Input: a directory containing the raw-package files:
#   - events.xml
#   - presentation/{presId}/svgs/*.svg
#   - presentation/{presId}/*.pdf
#   - presentation/{presId}/pdfs/**  (pre-generated breakout PDFs)
#   - notes/notes.*
#
# Output: artifacts-metadata.json (same format as Phase 1 dump), suitable for
# feeding back into post_publish_recording_artifacts.rb or any external PDF
# generation pipeline.
#
# This is the maintenance-heavy path discussed in the Phase 1 header of
# post_archive_recording_artifacts.rb. It involves stateful replay of tldraw
# shape events, SVG viewBox parsing for page dimensions, and XPath queries
# whose structure changes silently between BBB releases.
#
# PITFALLS:
#   - annotations() replays AddTldrawShapeEvent / DeleteTldrawShapeEvent in
#     document order to get final shape state. If BBB changes event ordering
#     or adds new shape mutation events, this breaks silently.
#   - page_dimensions() parses SVG viewBox attributes from the raw archive.
#     Assumes viewBox="0 0 W H" format. If BBB changes SVG generation (e.g.,
#     adds transforms or changes coordinate systems), dimensions will be wrong
#     and annotations will render misaligned.
#

require "optimist"
require "json"
require "fileutils"
require "nokogiri"
require "logger"

opts = Optimist::options do
  opt :input_dir, "Directory containing raw-package files (events.xml, presentation/, notes/)", type: String, required: true
  opt :output_file, "Output path for artifacts-metadata.json", type: String, default: nil
  opt :log_file, "Log file path", type: String, default: nil
  opt :verbose, "Enable verbose logging", default: false
end

input_dir = opts[:input_dir]
output_file = opts[:output_file] || File.join(input_dir, "artifacts-metadata.json")

if opts[:log_file]
  logger = Logger.new(opts[:log_file])
else
  logger = Logger.new($stderr)
end
logger.level = opts[:verbose] ? Logger::DEBUG : Logger::INFO

# ---------------------------------------------------------------------------
# Events.xml parser
# ---------------------------------------------------------------------------

module EventsXmlFallback
  def self.meeting_info(events)
    meeting_el = events.at_xpath("/recording/meeting")
    breakout_el = events.at_xpath("/recording/breakout")
    breakout_rooms = events.xpath("/recording/breakoutRooms/breakoutRoom").map { |r| r.content.strip }

    {
      "meetingId" => meeting_el["id"],
      "extId" => meeting_el["externalId"],
      "name" => meeting_el["name"],
      "isBreakout" => meeting_el["breakout"] == "true",
      "parentMeetingId" => breakout_el ? breakout_el["parentMeetingId"] : nil,
      "sequence" => breakout_el ? breakout_el["sequence"] : nil,
      "breakoutRooms" => breakout_rooms,
    }
  end

  def self.participants(events)
    users = {}
    events.xpath('/recording/event[@eventname="ParticipantJoinEvent"]').each do |event|
      uid = event.at_xpath("userId")&.content
      next unless uid
      next if users.key?(uid)
      users[uid] = {
        "userId" => uid,
        "extId" => event.at_xpath("externalUserId")&.content,
        "name" => event.at_xpath("name")&.content,
      }
    end
    users.values
  end

  def self.presentations(events, raw_dir)
    presentations = {}
    events.xpath('/recording/event[@eventname="ConversionCompletedEvent"]').each do |event|
      pid = event.at_xpath("presentationName")&.content
      next unless pid
      presentations[pid] = { "presentationId" => pid, "name" => event.at_xpath("originalFilename")&.content || pid }
    end
    if presentations.empty?
      pres_dir = File.join(raw_dir, "presentation")
      if File.directory?(pres_dir)
        Dir.children(pres_dir).each do |pid|
          next unless File.directory?(File.join(pres_dir, pid))
          presentations[pid] = { "presentationId" => pid, "name" => pid }
        end
      end
    end
    presentations
  end

  def self.annotations(events)
    annotations = {}
    events.xpath('/recording/event[@eventname="AddTldrawShapeEvent"]').each do |event|
      pid = event.at_xpath("presentation")&.content
      pnum = event.at_xpath("pageNumber")&.content&.to_i
      sid = event.at_xpath("shapeId")&.content
      raw = event.at_xpath("shapeData")&.content
      uid = event.at_xpath("userId")&.content
      next unless pid && pnum && sid && raw
      data = JSON.parse(raw) rescue next
      wb_id = event.at_xpath("whiteboardId")&.content || "#{pid}/#{pnum}"
      annotations[pid] ||= {}
      annotations[pid][pnum] ||= {}
      annotations[pid][pnum][sid] = { "id" => sid, "annotationInfo" => data, "wbId" => wb_id, "userId" => uid }
    end
    events.xpath('/recording/event[@eventname="DeleteTldrawShapeEvent"]').each do |event|
      pid = event.at_xpath("presentation")&.content
      pnum = event.at_xpath("pageNumber")&.content&.to_i
      sid = event.at_xpath("shapeId")&.content
      next unless pid && pnum && sid
      annotations.dig(pid, pnum)&.delete(sid)
    end
    result = {}
    annotations.each do |pid, pages|
      result[pid] = {}
      pages.each { |pnum, shapes| result[pid][pnum] = shapes.values }
    end
    result
  end

  def self.page_dimensions(raw_dir, pres_id)
    svgs_dir = File.join(raw_dir, "presentation", pres_id, "svgs")
    pages = {}
    return pages unless File.directory?(svgs_dir)
    Dir.glob(File.join(svgs_dir, "slide*.svg")).each do |svg|
      pnum = File.basename(svg, ".svg").sub("slide", "").to_i
      next if pnum < 1
      header = File.read(svg, 1024) rescue next
      if (m = header.match(/viewBox="0\s+0\s+(\d+(?:\.\d+)?)\s+(\d+(?:\.\d+)?)"/))
        pages[pnum] = { "width" => m[1].to_f, "height" => m[2].to_f }
      end
    end
    pages
  end
end

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

unless File.directory?(input_dir)
  logger.error("Input directory does not exist: #{input_dir}")
  exit 1
end

events_xml_path = File.join(input_dir, "events.xml")
unless File.exist?(events_xml_path)
  logger.error("events.xml not found in #{input_dir}")
  exit 1
end

logger.info("Processing events.xml from #{input_dir}")

events = Nokogiri::XML(File.open(events_xml_path))
meeting_info = EventsXmlFallback.meeting_info(events)
participants = EventsXmlFallback.participants(events)
mid = meeting_info["meetingId"]

logger.info("Meeting: #{mid} (breakout=#{meeting_info['isBreakout']})")

# Build presentations with pages and annotations
annotations_by_pres = EventsXmlFallback.annotations(events)
raw_presentations = EventsXmlFallback.presentations(events, input_dir)

presentations = raw_presentations.values.map do |p|
  pid = p["presentationId"]
  page_dims = EventsXmlFallback.page_dimensions(input_dir, pid)
  page_anns = annotations_by_pres[pid] || {}
  all_pages = (page_dims.keys + page_anns.keys).uniq.sort

  pages = all_pages.map do |pnum|
    dims = page_dims[pnum] || { "width" => 1920.0, "height" => 1080.0 }
    {
      "page" => pnum,
      "width" => dims["width"],
      "height" => dims["height"],
      "pageId" => "#{pid}/#{pnum}",
      "annotations" => page_anns[pnum] || [],
    }
  end

  total_anns = pages.sum { |pg| pg["annotations"].length }
  logger.info("  Presentation #{pid}: #{pages.length} page(s), #{total_anns} annotation(s)")

  p.merge("pages" => pages, "totalPages" => pages.length)
end

# Assemble output in the same format as Phase 1 dump
dump = {
  "version" => 1,
  "dumpedAt" => Time.now.iso8601,
  "source" => "events.xml",
  "meetingId" => mid,
  "meeting" => {
    "meetingId" => mid,
    "extId" => meeting_info["extId"],
    "isBreakout" => meeting_info["isBreakout"],
    "parentMeetingId" => meeting_info["parentMeetingId"],
    "sequence" => meeting_info["sequence"],
  },
  "breakouts" => meeting_info["breakoutRooms"].map.with_index(1) { |br_mid, i|
    { "meetingId" => br_mid, "sequence" => i }
  },
  "users" => participants,
  "breakoutUsers" => {},
  "presentations" => presentations,
  "sharedNotes" => nil,
}

total_annotations = presentations.sum { |p| (p["pages"] || []).sum { |pg| (pg["annotations"] || []).length } }

tmp = "#{output_file}.tmp"
File.write(tmp, JSON.pretty_generate(dump) + "\n")
File.rename(tmp, output_file)

logger.info(
  "Events.xml fallback for [#{mid}]: " \
  "#{presentations.length} presentation(s), #{total_annotations} annotation(s), " \
  "#{participants.length} user(s) -> #{output_file}"
)

exit 0
