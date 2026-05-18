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
# Phase 1: post_archive hook — Postgres snapshot
#
# bbb-export-annotations renders annotated slide PDFs. It needs
# presentation metadata, page dimensions, and tldraw annotation shapes.
# During a live meeting this data lives in Postgres (bbb_graphql), but
# all meeting tables are UNLOGGED and cascade-delete from the "meeting"
# row ~60 min after the meeting ends (DestroyMeetingInternalMsg in
# BigBlueButtonActor.scala).
#
# This hook snapshots the
# data Phase 2 needs into raw/{meetingId}/artifacts-metadata.json, which
# persists with the raw archive indefinitely.
#
# Output: meeting context, presentations with pages + annotations, user
# lists (for access manifests), breakout room assignments.
#
# The queries and data shapes here were derived by reverse-engineering
# bbb-export-annotations. The chain is: akka-bbb-apps constructs Redis
# payloads (StoreExportJobInRedisPresAnnEvent.scala, StoreAnnotations-
# InRedisPresAnnEvent.scala) → master.js pops from the "exportJobs"
# queue → collector.js reads annotations via hGetAll(jobId) → process.js
# overlays them on SVGs and writes the PDF to {presLocation}/pdfs/
# {jobId}/{serverSideFilename}.pdf. The Postgres schema is in
# bbb-graphql-server/bbb_schema.sql (pres_presentation, pres_page,
# pres_annotation, breakoutRoom_user).
#
# Some additional repos you can reference are: bbb-presentation-video, bbb-export-video and bbb-export-annotations
# These all handle slides generation in some shape or form.

require "optimist"
require "json"
require "fileutils"
require "pg"                # Only Phase 1 needs pg; Phase 2 is Postgres-free
require "time"
require File.expand_path('../../../lib/recordandplayback', __FILE__)

# Postgres connection params are in the bbb-apps-akka HOCON config, not a
# properties file. The "postgres" block contains serverName, portNumber,
# databaseName, user, password — all double-quoted string values.
BBB_APPS_AKKA_CONFIG = "/usr/share/bbb-apps-akka/conf/application.conf"

opts = Optimist::options do
  opt :meeting_id, "Meeting id to archive", type: String
  # post_archive hooks receive -m only (no -f format), unlike post_publish.
  # Optimist will ignore unknown flags, so this is safe even if the caller
  # passes extra args in the future.
end
meeting_id = opts[:meeting_id]

# All paths come from bigbluebutton.yml (with /etc/bigbluebutton/recording/
# recording.yml overrides), same as notes.rb, video.rb, etc. 
props = BigBlueButton.read_props
recording_dir = props["recording_dir"]
log_dir = props["log_dir"]

logger = Logger.new("#{log_dir}/post_archive.log", 'weekly')
logger.level = Logger::INFO
BigBlueButton.logger = logger

# The raw archive lives at {recording_dir}/raw/{meetingId}/ and is created by the
# archive step before this hook runs. It persists until explicitly deleted.
raw_dir = File.join(recording_dir, "raw", meeting_id)
output_file = File.join(raw_dir, "artifacts-metadata.json")
fail_file = File.join(raw_dir, "artifacts-metadata.fail")

BigBlueButton.logger.info("Phase 1 artifacts snapshot for [#{meeting_id}] starts")

def parse_hocon_properties(text, prefix)
  values = {}
  capture = false
  depth = 0
  text.each_line do |raw_line|
    line = raw_line.split("#", 2).first.strip   # strip inline comments
    next if line.empty?
    if !capture && line.start_with?(prefix) && line.include?("{")
      capture = true
      depth = line.count("{") - line.count("}")
      next
    end
    if capture
      depth += line.count("{") - line.count("}")
      if (m = line.match(/^([A-Za-z0-9_.-]+)\s*=\s*"([^"]*)"/))
        values[m[1]] = m[2]
      end
      break if depth <= 0
    end
  end
  values
end

# Read Postgres connection params from the bbb-apps-akka HOCON config.
# Returns a hash suitable for PG.connect, or nil if any param is missing.
# Current known defaults: host=127.0.0.1, port=5432, db=bbb_graphql,
# user=bbb_core, password=bbb_core — but we read them dynamically in case
# the deployment overrides them.
def detect_postgres_params
  unless File.exist?(BBB_APPS_AKKA_CONFIG)
    BigBlueButton.logger.warn("Postgres config not found at #{BBB_APPS_AKKA_CONFIG}")
    return nil
  end
  props = parse_hocon_properties(File.read(BBB_APPS_AKKA_CONFIG), "postgres")
  required = { "serverName" => props["serverName"], "portNumber" => props["portNumber"],
               "databaseName" => props["databaseName"], "user" => props["user"],
               "password" => props["password"] }
  missing = required.select { |_, v| v.nil? || v.empty? }.keys
  unless missing.empty?
    BigBlueButton.logger.warn("Postgres config in #{BBB_APPS_AKKA_CONFIG} is missing/empty: #{missing.join(', ')}")
    return nil
  end
  { host: required["serverName"], port: required["portNumber"].to_i,
    dbname: required["databaseName"], user: required["user"], password: required["password"] }
end

# Every Postgres query in this script returns a single JSON value (either a
# JSON object via row_to_json or a JSON array via json_agg). This helper
# executes the query and parses that single cell. Returns nil if no rows
# or empty result — callers use || [] for array queries.
#
# Uses exec_params with $1/$2/… placeholders instead of string interpolation.
# This avoids any dependency on escape_literal (whose availability changed
# between pg gem versions) and is immune to SQL injection by construction.
def pg_json(conn, sql, params = [])
  result = conn.exec_params(sql, params)
  return nil if result.ntuples == 0
  raw = result.getvalue(0, 0)
  return nil if raw.nil? || raw.strip.empty?
  JSON.parse(raw)
end

# SQL snippet that formats a timestamptz column as ISO 8601 UTC (Z-suffix).
# Postgres' default JSON serialization of timestamptz uses the session
# timezone, which is not what we want — downstream consumers expect UTC.
# Used for both startedAt (from createdTime via to_timestamp) and endedAt
# (already timestamptz).
def iso_utc(expr)
  %(to_char((#{expr}) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'))
end

begin
  pg_params = detect_postgres_params
  raise "Cannot detect Postgres connection parameters from #{BBB_APPS_AKKA_CONFIG}" unless pg_params

  BigBlueButton.logger.info("Connecting to Postgres #{pg_params[:user]}@#{pg_params[:host]}:#{pg_params[:port]}/#{pg_params[:dbname]}")
  conn = PG.connect(pg_params)
  BigBlueButton.logger.debug("Postgres connection established for [#{meeting_id}]")

  # ---------------------------------------------------------------------------
  # Meeting context
  # ---------------------------------------------------------------------------
  # The LEFT JOIN on meeting_breakoutRoomProps distinguishes breakout rooms
  # from parent meetings. parentMeetingId is set for breakouts but has the
  # sentinel value 'bbb-none' for parent meetings — hence the explicit check.
  #
  # startedAt/endedAt are sourced from meeting.createdTime (bigint epoch ms,
  # written by bbb-apps-akka on meeting creation) and meeting.endedAt
  # (timestamptz, written when the meeting ends). Both formatted as ISO 8601
  # UTC strings. endedAt may be null in pathological cases where post_archive
  # runs before the end is recorded — downstream consumers should tolerate.
  meeting_ctx = pg_json(conn, <<~SQL, [meeting_id])
    SELECT row_to_json(t)::text FROM (
      SELECT m."meetingId", m."extId", m."name",
             mbp."parentMeetingId", mbp."sequence",
             CASE WHEN mbp."parentMeetingId" IS NOT NULL
                   AND mbp."parentMeetingId" <> 'bbb-none'
                  THEN true ELSE false END AS "isBreakout",
             #{iso_utc("to_timestamp(m.\"createdTime\" / 1000.0)")} AS "startedAt",
             #{iso_utc('m."endedAt"')} AS "endedAt"
      FROM "meeting" m
      LEFT JOIN "meeting_breakoutRoomProps" mbp USING ("meetingId")
      WHERE m."meetingId" = $1
      LIMIT 1
    ) t;
  SQL

  unless meeting_ctx
    BigBlueButton.logger.info("Meeting #{meeting_id} not found in Postgres, skipping snapshot")
    exit 0
  end

  BigBlueButton.logger.info("Meeting times for [#{meeting_id}]: #{meeting_ctx['startedAt']} -> #{meeting_ctx['endedAt']}")

  # ---------------------------------------------------------------------------
  # Breakout rooms (only for parent meetings)
  # ---------------------------------------------------------------------------
  # v_breakoutRoom_createdLatest is a view that returns the most recent
  # breakout room creation per sequence number. We only query this from the
  # parent meeting — breakout rooms don't have nested breakouts.
  #
  # JOIN "meeting" to pull the breakout's external id and timestamps in one
  # query. INNER JOIN is correct: a breakout row in the view must have a
  # matching meeting row (the view is built from breakoutRoom which has an
  # FK to meeting). If a breakout was declared but never spun up
  # (no meeting row), we won't see it here — that's the same gap v3 had.
  breakouts = []
  unless meeting_ctx["isBreakout"]
    breakouts = pg_json(conn, <<~SQL, [meeting_id]) || []
      SELECT COALESCE(json_agg(json_build_object(
        'meetingId', br."breakoutRoomMeetingId",
        'extId',     m."extId",
        'sequence',  br."sequence",
        'name',      br."name",
        'startedAt', #{iso_utc("to_timestamp(m.\"createdTime\" / 1000.0)")},
        'endedAt',   #{iso_utc('m."endedAt"')}
      ) ORDER BY br."sequence"), '[]'::json)::text
      FROM "v_breakoutRoom_createdLatest" br
      JOIN "meeting" m ON m."meetingId" = br."breakoutRoomMeetingId"
      WHERE br."meetingId" = $1;
    SQL
  end

  # ---------------------------------------------------------------------------
  # Users (for access manifests)
  # ---------------------------------------------------------------------------
  # extId is the external user ID passed by the integration (e.g., Greenlight,
  # Moodle). This is what downstream systems use to match BBB users to their
  # own user records. userId is BBB's internal ID (w_xxxxx format).
  # isModerator is a generated column on "user" (bbb_schema.sql ~L381):
  # GENERATED ALWAYS AS (CASE WHEN "role" = 'MODERATOR' THEN true ELSE false END).
  users = pg_json(conn, <<~SQL, [meeting_id]) || []
    SELECT COALESCE(json_agg(json_build_object(
      'userId', u."userId", 'extId', u."extId", 'name', u."name",
      'isModerator', u."isModerator"
    ) ORDER BY u."userId"), '[]'::json)::text
    FROM "user" u WHERE u."meetingId" = $1;
  SQL

  # ---------------------------------------------------------------------------
  # Breakout room user assignments (for scoped access)
  # ---------------------------------------------------------------------------
  # breakoutRoom_user maps parent-meeting users to the breakout rooms they
  # joined. We query from the parent meeting's "user" table (JOIN condition)
  # filtered by each breakout's meetingId. This gives us the user details
  # (name, extId) for each breakout's access manifest.
  #
  # PITFALL: This is one query per breakout room. For meetings with many
  # breakouts this could be slow — but in practice BBB limits breakout rooms
  # to ~16 and this whole script runs in <100ms total.
  breakout_users = {}
  breakouts.each do |br|
    br_mid = br["meetingId"]
    br_users = pg_json(conn, <<~SQL, [br_mid]) || []
      SELECT COALESCE(json_agg(json_build_object(
        'userId', u."userId", 'extId', u."extId", 'name', u."name",
        'isModerator', u."isModerator"
      ) ORDER BY u."userId"), '[]'::json)::text
      FROM "breakoutRoom_user" bru
      JOIN "user" u ON u."meetingId" = bru."meetingId" AND u."userId" = bru."userId"
      WHERE bru."breakoutRoomMeetingId" = $1;
    SQL
    breakout_users[br_mid] = br_users
  end

  # ---------------------------------------------------------------------------
  # Presentations with pages and annotations
  # ---------------------------------------------------------------------------
  # We dump ALL presentations, not just the current one, because users may
  # have annotated slides on a previous presentation before switching.
  # Ordered by createdAt DESC so the most recent (likely current) is first.
  presentations = pg_json(conn, <<~SQL, [meeting_id]) || []
    SELECT COALESCE(json_agg(json_build_object(
      'presentationId', p."presentationId",
      'name', p."name",
      'current', p."current",
      'totalPages', p."totalPages"
    ) ORDER BY p."createdAt" DESC), '[]'::json)::text
    FROM "pres_presentation" p
    WHERE p."meetingId" = $1;
  SQL

  # For each presentation, fetch pages with their annotations inlined.
  # This is the core data that bbb-export-annotations needs to render PDFs.
  #
  # The output shape of each annotation object must match what
  # bbb-export-annotations/workers/collector.js expects when it reads
  # the annotations hash from Redis (hGetAll on jobId). Specifically:
  #   - "id"             → annotation identifier (shape:xxx)
  #   - "annotationInfo" → raw tldraw shape JSON (passed through verbatim)
  #   - "wbId"           → whiteboard/page ID (pageId from pres_page)
  #   - "userId"         → who created the annotation
  # See: StoreAnnotationsInRedisPresAnnEvent.scala for the Scala side,
  #      workers/process.js lines ~321-327 for how they're consumed.
  #
  # PITFALL: annotationInfo is stored as a TEXT column containing JSON (not
  # a jsonb column) — see bbb_schema.sql line ~1619. The ::json cast can
  # fail if the data is malformed, but in practice bbb-graphql-server
  # validates it on write. The btrim check filters out empty-string
  # annotations that occasionally appear after undo operations.
  #
  # PITFALL: We use pageId as the "wbId" value. In the live application,
  # the whiteboard ID format is "{presId}/{pageNumber}", but the Postgres
  # pres_annotation table stores the pageId directly. bbb-export-annotations
  # accepts either format — it uses wbId only for grouping, not path
  # construction.
  presentations.each do |pres|
    pres_id = pres["presentationId"]
    pages = pg_json(conn, <<~SQL, [pres_id]) || []
      SELECT COALESCE(json_agg(page_row ORDER BY (page_row->>'page')::int), '[]'::json)::text
      FROM (
        SELECT json_build_object(
          'page', pp."num",
          'width', pp."width",
          'height', pp."height",
          'pageId', pp."pageId",
          'annotations', COALESCE(
            (SELECT json_agg(json_build_object(
              'id', pa."annotationId",
              'annotationInfo', pa."annotationInfo"::json,
              'wbId', pa."pageId",
              'userId', pa."userId"
            ) ORDER BY pa."lastUpdatedAt")
            FROM "pres_annotation" pa
            WHERE pa."pageId" = pp."pageId"
              AND pa."annotationInfo" IS NOT NULL
              AND btrim(pa."annotationInfo") <> ''),
            '[]'::json)
        ) AS page_row
        FROM "pres_page" pp
        WHERE pp."presentationId" = $1
          AND pp."uploadCompleted" IS TRUE
      ) s;
    SQL
    pres["pages"] = pages
  end

  # ---------------------------------------------------------------------------
  # Assemble and write dump
  # ---------------------------------------------------------------------------
  # Version field allows Phase 2 to handle format changes gracefully.
  # Write to .tmp first then atomic rename to avoid Phase 2 reading a
  # partial file if it happens to run concurrently (shouldn't happen in
  # normal flow, but defensive).
  dump = {
    "version" => 1,
    "dumpedAt" => Time.now.iso8601,
    "meetingId" => meeting_id,
    "meeting" => meeting_ctx,
    "breakouts" => breakouts,
    "users" => users,
    "breakoutUsers" => breakout_users,
    "presentations" => presentations,
  }

  tmp = "#{output_file}.tmp"
  File.write(tmp, JSON.pretty_generate(dump) + "\n")
  File.rename(tmp, output_file)
  FileUtils.rm_f(fail_file)

  total_annotations = presentations.sum { |p| (p["pages"] || []).sum { |pg| (pg["annotations"] || []).length } }
  BigBlueButton.logger.info(
    "Phase 1 snapshot for [#{meeting_id}]: " \
    "#{presentations.length} presentation(s), #{total_annotations} annotation(s), " \
    "#{users.length} user(s), #{breakouts.length} breakout(s) -> #{output_file}"
  )

  # ---------------------------------------------------------------------------
  # Verify presentation sources exist in raw archive
  # ---------------------------------------------------------------------------
  # Phase 2 needs SVGs and the original PDF to upload rebuild sources to S3.
  # The archive step copies these to raw/{meetingId}/presentation/{presId}/,
  # so they should always be present. Log warnings if anything is missing so
  # Phase 2 issues are diagnosable from the Phase 1 log.
  pres_dir = File.join(raw_dir, "presentation")
  presentations.each do |pres|
    pres_id = pres["presentationId"]
    svgs_dir = File.join(pres_dir, pres_id, "svgs")
    unless File.directory?(svgs_dir) && Dir.glob(File.join(svgs_dir, "slide*.svg")).any?
      BigBlueButton.logger.warn("Missing SVGs in raw archive for #{pres_id} in [#{meeting_id}]")
    end
    unless Dir.glob(File.join(pres_dir, pres_id, "*.pdf")).any?
      BigBlueButton.logger.warn("Missing original PDF in raw archive for #{pres_id} in [#{meeting_id}]")
    end
  end

rescue => e
  BigBlueButton.logger.error("Phase 1 snapshot for [#{meeting_id}] failed: #{e.class}: #{e.message}")
  BigBlueButton.logger.error(e.backtrace.first(10).join("\n")) if e.backtrace
  begin
    FileUtils.mkdir_p(raw_dir)
    File.write(fail_file, JSON.pretty_generate({
      "meeting_id" => meeting_id,
      "timestamp" => Time.now.iso8601,
      "error_class" => e.class.to_s,
      "error" => e.message,
    }) + "\n")
    BigBlueButton.logger.info("Wrote Phase 1 failure marker: #{fail_file}")
  rescue => marker_error
    BigBlueButton.logger.warn("Could not write #{fail_file}: #{marker_error.class}: #{marker_error.message}")
  end
ensure
  conn&.close
end

BigBlueButton.logger.info("Phase 1 artifacts snapshot for [#{meeting_id}] ends")

exit 0
