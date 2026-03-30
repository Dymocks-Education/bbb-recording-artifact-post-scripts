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
# Phase 2: post_publish hook — artifact generation
#
# Reads the Postgres snapshot from Phase 1 (artifacts-metadata.json in
# the raw archive) and uses it to generate annotated slide PDFs and
# access-scoped artifact copies. No Postgres dependency — by the time
# this hook runs (after process:video/notes), the meeting data is long
# gone from bbb_graphql.
#
# Annotated PDFs are rendered by bbb-export-annotations. We push jobs
# to its Redis queue ("exportJobs" list + annotation hash keyed by
# jobId) and poll for the output PDF. The job format and annotation
# shape contract are the same as the live application uses — derived
# from StoreExportJobInRedisPresAnnEvent.scala and process.js (see
# Phase 1 header for the full derivation chain).
#
# For breakout rooms, the "Capture Slides" option in the breakout room
# creation dialog (bigbluebutton-html5 CreateBreakoutRoom component)
# tells the breakout to render its annotated whiteboard as a PDF and
# send it back to the parent room when the breakout ends. These
# pre-generated PDFs are archived at raw/{id}/presentation/*/pdfs/.
# When present we copy them directly instead of re-rendering via
# bbb-export-annotations — this is the fast path for breakouts.
#
# If the Phase 1 dump is missing (Postgres was unavailable at archive
# time), we package the raw files needed for external PDF generation
# (events.xml, presentation SVGs/PDFs) into the output directory.
# The events.xml fallback logic lives in a separate standalone script
# (export_recording_artifacts_eventsxml.rb) that can be run externally
# in a controlled environment. This keeps the brittle stateful replay
# code out of the recording pipeline.
#
# Dev/prod mode is detected from meeting metadata. This controls output
# directory, log verbosity, retry/timeout behavior. The intent is that
# the same code runs on dev and prod servers without config changes —
# mode is inferred from bbb-origin-server-name in events.xml (set by
# the integration, e.g., Greenlight passes the server hostname) or an
# explicit artifactExportMode metadata key on the BBB create API call.
#

require "optimist"
require "json"
require "fileutils"
require "redis"
require File.expand_path('../../../lib/recordandplayback', __FILE__)

CONFIG_FILE = "/etc/default/bbb-recording-artifacts"

# All paths come from bigbluebutton.yml (with /etc/bigbluebutton/recording/
# recording.yml overrides), same as notes.rb, video.rb, etc. Never hardcode
# /var/bigbluebutton or /var/log/bigbluebutton — deployments can override these.
BBB_PROPS = BigBlueButton.read_props

# Mode-specific defaults. Output dirs are relative to raw_presentation_src
# (typically /var/bigbluebutton) so they follow deployment overrides.
MODE_DEFAULTS = {
  "dev" => {
    "output_dir"    => "#{BBB_PROPS['raw_presentation_src']}/recording-artifacts-dev",
    "wait_timeout"  => 120,
    "poll_interval" => 1,
    "retry_max"     => 1,
    "retry_delay"   => 1,
    "log_level"     => Logger::DEBUG,
  },
  "prod" => {
    "output_dir"    => "#{BBB_PROPS['raw_presentation_src']}/recording-artifacts",
    "wait_timeout"  => 180,
    "poll_interval" => 2,
    "retry_max"     => 3,
    "retry_delay"   => 2,
    "log_level"     => Logger::INFO,
  },
}.freeze

opts = Optimist::options do
  opt :meeting_id, "Meeting id to archive", type: String
  opt :format, "Playback format name", type: String
end
meeting_id = opts[:meeting_id]

# Per-meeting log
log_dir = BBB_PROPS["log_dir"]
begin
  FileUtils.mkdir_p(log_dir)
  logger = Logger.new(File.join(log_dir, "recording-artifacts-#{meeting_id}.log"), "daily")
rescue => e
  logger = Logger.new(File.join(log_dir, "post_publish.log"), "weekly")
  logger.warn("Could not create per-meeting log for #{meeting_id}: #{e.message}")
end
logger.level = Logger::INFO
BigBlueButton.logger = logger

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def load_env_config
  config = {}
  if File.exist?(CONFIG_FILE)
    File.readlines(CONFIG_FILE).each do |line|
      line = line.strip
      next if line.empty? || line.start_with?("#")
      key, value = line.split("=", 2)
      config[key.strip] = value.strip if key && value
    end
  end
  config
end

def get_metadata(key, meeting_metadata)
  meeting_metadata.key?(key) ? meeting_metadata[key].value : nil
end

# Mode detection priority:
#   1. Explicit metadata: artifactExportMode=dev|prod (set via BBB API
#      create call meta_artifactExportMode=dev). Most reliable, but
#      requires the integration to set it.
#   2. Config file: BBB_RECORDING_ARTIFACTS_MODE in /etc/default/
#      bbb-recording-artifacts. Useful for forcing mode on a server.
#   3. Default: prod (safe default — prod is stricter).
def detect_mode(meeting_metadata, config)
  meta_mode = get_metadata("artifactExportMode", meeting_metadata)
  return meta_mode if meta_mode && %w[dev prod].include?(meta_mode)

  config_mode = config["BBB_RECORDING_ARTIFACTS_MODE"]
  return config_mode if config_mode && %w[dev prod].include?(config_mode)

  "prod"
end

def parse_positive_integer(value, default)
  parsed = Integer(value)
  parsed.positive? ? parsed : default
rescue ArgumentError, TypeError
  default
end

def sanitize_filename(name)
  value = name.strip.gsub(/\s+/, "_")
  value = value.gsub(/[^A-Za-z0-9._-]/, "_")
  value = value.gsub(/_+/, "_")
  value = value.gsub(/\A[._]+|[._]+\z/, "")
  value.empty? ? "artifact" : value
end

# ---------------------------------------------------------------------------
# Exporter
# ---------------------------------------------------------------------------

class RecordingArtifactsExporter
  def initialize(meeting_id, format, mode, config, logger)
    @meeting_id = meeting_id
    @format = format
    @mode = mode
    @logger = logger

    mode_defaults = MODE_DEFAULTS[@mode]
    @output_dir     = config["BBB_RECORDING_ARTIFACTS_OUTPUT_DIR"]     || mode_defaults["output_dir"]
    @wait_timeout   = parse_positive_integer(config["BBB_RECORDING_ARTIFACTS_WAIT_TIMEOUT"],  mode_defaults["wait_timeout"])
    @poll_interval  = parse_positive_integer(config["BBB_RECORDING_ARTIFACTS_POLL_INTERVAL"], mode_defaults["poll_interval"])
    @retry_max      = parse_positive_integer(config["BBB_RECORDING_ARTIFACTS_RETRY_MAX"],     mode_defaults["retry_max"])
    @retry_delay    = parse_positive_integer(config["BBB_RECORDING_ARTIFACTS_RETRY_DELAY"],   mode_defaults["retry_delay"])
    @include_breakouts = config["BBB_RECORDING_ARTIFACTS_INCLUDE_BREAKOUTS"] != "false"
    @dry_run           = config["BBB_RECORDING_ARTIFACTS_DRY_RUN"] == "true"

    # All paths from bigbluebutton.yml, same as notes.rb / video.rb.
    # raw_presentation_src is the live presentation root (/var/bigbluebutton)
    # where bbb-web stores uploaded slides at {root}/{mid}/{mid}/{presId}/.
    # recording_dir is where the recording pipeline stores raw/process/status.
    @artifact_root = BBB_PROPS["raw_presentation_src"]
    @recording_dir = BBB_PROPS["recording_dir"]
    @published_status_dir = File.join(@recording_dir, "status", "published")

    FileUtils.mkdir_p(@output_dir)
    FileUtils.mkdir_p(@published_status_dir)

    @logger.level = mode_defaults["log_level"]
  end

  def run
    with_export_lock do
      if File.exist?(done_file)
        @logger.info("Artifact export already completed for #{@meeting_id}, skipping")
        next []
      end

      raw_dir = File.join(@recording_dir, "raw", @meeting_id)
      events_xml_path = File.join(raw_dir, "events.xml")

      unless File.exist?(events_xml_path)
        @logger.info("No events.xml for #{@meeting_id}, skipping")
        next []
      end

      dump = load_phase1_dump(raw_dir)

      exports = []
      errors = []

      if dump
        @logger.info("Using Phase 1 Postgres dump for #{@meeting_id} (mode=#{@mode})")
        export_from_dump(raw_dir, dump, exports, errors)
      else
        @logger.info("No Phase 1 dump for #{@meeting_id}, packaging raw files for external processing (mode=#{@mode})")
        package_raw_files(raw_dir, exports, errors)
      end

      if errors.empty?
        FileUtils.touch(done_file)
        FileUtils.rm_f(fail_file)
        @logger.info("Artifact export complete for #{@meeting_id}: #{exports.length} artifact(s) (mode=#{@mode})")
      elsif exports.any?
        write_fail_file(errors)
        @logger.warn("Artifact export partial for #{@meeting_id}: #{exports.length} ok, #{errors.length} failed")
      else
        write_fail_file(errors)
        @logger.error("Artifact export failed for #{@meeting_id}: all artifacts failed")
      end

      exports
    end
  end

  private

  # --- Retry helper ---

  def with_retries(label)
    attempts = 0
    begin
      attempts += 1
      yield
    rescue => e
      if attempts < @retry_max
        delay = @retry_delay * (2 ** (attempts - 1))
        @logger.warn("#{label} failed (attempt #{attempts}/#{@retry_max}): #{e.message}, retrying in #{delay}s")
        sleep(delay)
        retry
      end
      raise
    end
  end

  # --- Status/lock files ---
  # post_publish hooks run once per published format (video, notes, etc.).
  # Multiple formats can publish concurrently, so the lock prevents
  # duplicate artifact exports. The done file prevents re-export on
  # subsequent format publishes or manual re-runs.

  def done_file
    File.join(@published_status_dir, "#{@meeting_id}-recording-artifacts.done")
  end

  def fail_file
    File.join(@published_status_dir, "#{@meeting_id}-recording-artifacts.fail")
  end

  def lock_file
    File.join(@published_status_dir, "#{@meeting_id}-recording-artifacts.lock")
  end

  def write_fail_file(errors)
    File.write(fail_file, JSON.pretty_generate({
      "meeting_id" => @meeting_id, "mode" => @mode,
      "timestamp" => Time.now.iso8601, "errors" => errors,
    }) + "\n")
  end

  def with_export_lock
    File.open(lock_file, File::RDWR | File::CREAT, 0o644) do |f|
      f.flock(File::LOCK_EX)
      yield
    ensure
      f.flock(File::LOCK_UN) rescue nil
    end
  end

  # --- Phase 1 dump loader ---

  def load_phase1_dump(raw_dir)
    path = File.join(raw_dir, "artifacts-metadata.json")
    return nil unless File.exist?(path)
    dump = JSON.parse(File.read(path))
    @logger.debug("Loaded Phase 1 dump: version=#{dump['version']}, dumped=#{dump['dumpedAt']}")
    dump
  rescue => e
    @logger.warn("Failed to load Phase 1 dump: #{e.message}")
    nil
  end

  # =========================================================================
  # Export from Phase 1 Postgres dump (preferred path)
  # =========================================================================

  def export_from_dump(raw_dir, dump, exports, errors)
    meeting_ctx = dump["meeting"]
    mid = meeting_ctx["meetingId"]

    # Export parent meeting
    export_meeting_from_dump(raw_dir, dump, meeting_ctx, dump["users"], exports, errors)

    # Export breakout rooms
    if @include_breakouts && !meeting_ctx["isBreakout"]
      (dump["breakouts"] || []).each do |breakout|
        br_mid = breakout["meetingId"]
        br_raw_dir = File.join(@recording_dir, "raw", br_mid)

        br_ctx = {
          "meetingId" => br_mid,
          "isBreakout" => true,
          "parentMeetingId" => mid,
          "sequence" => breakout["sequence"],
          "name" => breakout["name"],
        }
        br_users = (dump["breakoutUsers"] || {})[br_mid] || []
        export_meeting_from_dump(br_raw_dir, nil, br_ctx, br_users, exports, errors)
      end
    end
  end

  def export_meeting_from_dump(raw_dir, dump, meeting_ctx, authorized_users, exports, errors)
    mid = meeting_ctx["meetingId"]
    is_breakout = meeting_ctx["isBreakout"]

    begin
      if is_breakout
        existing = find_existing_annotated_pdfs(raw_dir)
        unless existing.empty?
          @logger.info("Found #{existing.length} pre-generated PDF(s) for breakout #{mid}")
          existing.each do |pdf_path|
            result = store_artifact("annotated-slides", pdf_path, meeting_ctx, authorized_users)
            exports << result if result
          end
          raise :skip_slides
        end
        @logger.info("No pre-generated PDFs for breakout #{mid}, generating via Redis")
      end

      presentations = dump ? (dump["presentations"] || []) : []

      if presentations.empty? && !is_breakout
        @logger.warn("No presentations in Phase 1 dump for #{mid}")
      end

      presentations.each do |pres|
        pres_id = pres["presentationId"]
        pages = pres["pages"] || []
        annotated_pages = pages.select { |p| p["annotations"] && !p["annotations"].empty? }

        if annotated_pages.empty?
          @logger.debug("No annotations on #{pres_id} in #{mid}, skipping")
          next
        end

        pres_location = resolve_pres_location(raw_dir, mid, pres_id)
        unless pres_location
          @logger.warn("No presentation files for #{pres_id} in #{mid}, skipping")
          next
        end

        begin
          pdf_path = generate_annotated_pdf(mid, pres_id, pres["name"], pres_location, pages)
          result = store_artifact("annotated-slides", pdf_path, meeting_ctx, authorized_users, pres["name"])
          exports << result if result
        rescue => e
          @logger.error("Annotated PDF failed for #{pres_id} in #{mid}: #{e.message}")
          @logger.error(e.backtrace.first(3).join("\n")) if e.backtrace
          errors << { "meeting_id" => mid, "type" => "annotated-slides", "presId" => pres_id, "error" => e.message }
        end
      end
    rescue => e
      raise unless e == :skip_slides
    end

  end

  # =========================================================================
  # Raw file packaging (no Phase 1 dump — for external processing)
  # =========================================================================
  #
  # Fallback to allow for generation externally using events.xml and other required files.
  #
  # Packaged files:
  #   - events.xml                         (event trail for annotation replay)
  #   - presentation/{presId}/svgs/*.svg   (base slides for PDF rendering)
  #   - presentation/{presId}/*.pdf        (original uploaded PDFs)
  #   - presentation/{presId}/pdfs/**      (pre-generated breakout PDFs)

  def package_raw_files(raw_dir, exports, errors)
    mid = @meeting_id
    package_dir = File.join(@output_dir, mid, "raw-package")
    FileUtils.mkdir_p(package_dir)

    files_copied = 0

    # events.xml
    events_src = File.join(raw_dir, "events.xml")
    if File.exist?(events_src)
      FileUtils.cp(events_src, File.join(package_dir, "events.xml"))
      files_copied += 1
      @logger.info("Packaged events.xml for #{mid}")
    else
      @logger.warn("No events.xml found in #{raw_dir}")
      errors << { "meeting_id" => mid, "type" => "raw-package", "error" => "events.xml missing" }
    end

    # artifacts-metadata.json (may exist but be corrupt — copy it anyway for debugging)
    metadata_src = File.join(raw_dir, "artifacts-metadata.json")
    if File.exist?(metadata_src)
      FileUtils.cp(metadata_src, File.join(package_dir, "artifacts-metadata.json"))
      files_copied += 1
    end

    # Presentation files (SVGs, original PDFs, pre-generated breakout PDFs)
    pres_src = File.join(raw_dir, "presentation")
    if File.directory?(pres_src)
      pres_dest = File.join(package_dir, "presentation")
      Dir.children(pres_src).each do |pres_id|
        pres_path = File.join(pres_src, pres_id)
        next unless File.directory?(pres_path)

        # SVGs
        svgs_src = File.join(pres_path, "svgs")
        if File.directory?(svgs_src)
          svgs_dest = File.join(pres_dest, pres_id, "svgs")
          FileUtils.mkdir_p(svgs_dest)
          Dir.glob(File.join(svgs_src, "*.svg")).each do |svg|
            FileUtils.cp(svg, svgs_dest)
            files_copied += 1
          end
        end

        # Original PDFs (at presentation root level)
        Dir.glob(File.join(pres_path, "*.pdf")).each do |pdf|
          pdf_dest = File.join(pres_dest, pres_id)
          FileUtils.mkdir_p(pdf_dest)
          FileUtils.cp(pdf, pdf_dest)
          files_copied += 1
        end

        # Pre-generated breakout PDFs
        pdfs_src = File.join(pres_path, "pdfs")
        if File.directory?(pdfs_src)
          pdfs_dest = File.join(pres_dest, pres_id, "pdfs")
          FileUtils.cp_r(pdfs_src, pdfs_dest)
          files_copied += 1
        end
      end
    end

    if files_copied > 0
      exports << { "artifact_type" => "raw-package", "meeting_id" => mid, "file" => package_dir }
      @logger.info("Packaged #{files_copied} raw file(s) for external processing: #{package_dir}")
    else
      @logger.warn("No raw files found to package for #{mid}")
      errors << { "meeting_id" => mid, "type" => "raw-package", "error" => "no files found" }
    end
  end

  # =========================================================================
  # Shared helpers
  # =========================================================================

  # bbb-export-annotations reads SVGs from {presLocation}/svgs/slide{N}.svg
  # and the original PDF from {presLocation}/{presId}.pdf.
  #
  # Two locations may have these files:
  #   1. Live dir: /var/bigbluebutton/{mid}/{mid}/{presId}/
  #      Still exists at post_archive time, may be cleaned up later by
  #      bbb-web's PresentationCleanupService.
  #   2. Raw archive: raw/{mid}/presentation/{presId}/
  #      Permanent copy created by the archive step.
  #
  # We prefer the live dir because bbb-export-annotations was designed
  # to work with it (same paths the live app uses). The raw archive is
  # the fallback. PITFALL: if the live dir is cleaned up between
  # post_archive and post_publish, and the raw archive SVGs have a
  # different layout, PDF rendering could fail.
  def resolve_pres_location(raw_dir, mid, pres_id)
    live = File.join(@artifact_root, mid, mid, pres_id)
    raw = File.join(raw_dir, "presentation", pres_id)
    if File.directory?(File.join(live, "svgs"))
      live
    elsif File.directory?(File.join(raw, "svgs"))
      raw
    end
  end

  # Find pre-generated annotated PDFs in a breakout room's raw archive.
  # These exist when the "Capture Slides" option was enabled in the
  # breakout room creation dialog (CreateBreakoutRoom component in
  # bigbluebutton-html5). The breakout renders its annotated whiteboard
  # as a PDF and sends it to the parent room before closing. The archive
  # step stores it at raw/{breakoutId}/presentation/{presId}/pdfs/.
  def find_existing_annotated_pdfs(raw_dir)
    pres_dir = File.join(raw_dir, "presentation")
    return [] unless File.directory?(pres_dir)
    Dir.glob(File.join(pres_dir, "*", "pdfs", "**", "*.pdf")).select do |pdf|
      File.size(pdf) > 0 && (File.binread(pdf, 5) rescue nil) == "%PDF-"
    end
  end

  # --- Annotated PDF via bbb-export-annotations ---

  # Push a job to bbb-export-annotations via Redis. This replicates what
  # akka-bbb-apps does during a live export request:
  #   1. Store annotation data as a Redis hash (key = jobId)
  #   2. Push the export job JSON to the "exportJobs" list
  # bbb-export-annotations' master.js blPops from the list, collector.js
  # reads the hash, process.js renders the PDF with GhostScript.
  #
  # The two-step pattern (hash then list push) is required because
  # master.js uses the list pop as the trigger — the hash must already
  # exist when collector.js reads it.
  #
  # Output path: {presLocation}/pdfs/{jobId}/{serverSideFilename}.pdf
  # (constructed by process.js, see workers/process.js lines ~439-448)
  def generate_annotated_pdf(mid, pres_id, pres_name, pres_location, pages)
    job_id = "#{mid}-artifacts-#{Time.now.to_i}"
    base_name = File.basename(pres_name, File.extname(pres_name))
    server_side_filename = sanitize_filename("annotated-#{base_name}")
    page_numbers = pages.map { |p| p["page"] }

    export_job = {
      "module" => "PRES-ANN",
      "eventName" => "StoreExportJobInRedisPresAnnEvent",
      "jobId" => job_id,
      "jobType" => "PresentationWithAnnotationDownloadJob",
      "filename" => pres_name,
      "serverSideFilename" => server_side_filename,
      "presId" => pres_id,
      "presLocation" => pres_location,
      "allPages" => "true",
      "pages" => JSON.generate(page_numbers),
      "parentMeetingId" => mid,
      "presentationUploadToken" => "",
    }

    annotations_payload = {
      "module" => "PRES-ANN",
      "eventName" => "StoreAnnotationsInRedisPresAnnEvent",
      "jobId" => job_id,
      "presId" => pres_id,
      "pages" => JSON.generate(pages),
    }

    output_path = File.join(pres_location, "pdfs", job_id, "#{server_side_filename}.pdf")

    if @dry_run
      @logger.info("[dry-run] Would queue job #{job_id} for #{pres_id}")
      FileUtils.mkdir_p(File.dirname(output_path))
      File.write(output_path, "%PDF-dry-run")
      return output_path
    end

    @logger.info("Queuing annotation export job #{job_id} for #{pres_id}")

    with_retries("Redis export job") do
      redis = Redis.new
      begin
        redis.del(job_id)
        annotations_payload.each { |k, v| redis.hset(job_id, k, v) }
        redis.rpush("exportJobs", JSON.generate(export_job))
      ensure
        redis.close
      end
    end

    wait_for_pdf(output_path)
    output_path
  end

  # bbb-export-annotations writes the PDF via GhostScript, which writes
  # incrementally. We check for the %PDF- magic header to confirm the
  # file is a complete, valid PDF — not a partial write or an error
  # message that GhostScript sometimes dumps to the output path.
  def wait_for_pdf(path)
    deadline = Time.now + @wait_timeout
    while Time.now < deadline
      if File.exist?(path) && File.size(path) > 0
        header = File.binread(path, 5)
        return path if header == "%PDF-"
        @logger.warn("File #{path} exists but not a valid PDF yet, waiting...")
      end
      sleep(@poll_interval)
    end
    raise "Timed out waiting for #{path} after #{@wait_timeout}s"
  end

  # --- Artifact storage and access manifests ---

  # Output layout:
  #   {output_dir}/{parentMeetingId}/annotated-slides/annotated-{name}.pdf
  #   {output_dir}/{parentMeetingId}/annotated-slides/annotated-{name}.pdf.access.json
  #   {output_dir}/{parentMeetingId}/breakouts/{seq}/annotated-slides/...
  #
  # Everything nests under the parent meeting ID so breakout artifacts
  # are grouped with their parent. The .access.json manifest lists which
  # users are authorized to view the artifact — scoped to the breakout
  # room's participants for breakout artifacts.
  def store_artifact(artifact_type, source_path, meeting_ctx, authorized_users, pres_name = nil)
    mid = meeting_ctx["meetingId"]
    parent_mid = meeting_ctx["isBreakout"] ? meeting_ctx["parentMeetingId"] : mid

    if meeting_ctx["isBreakout"]
      seq = meeting_ctx["sequence"] || mid
      artifact_prefix = File.join(parent_mid, "breakouts", seq.to_s, artifact_type)
    else
      artifact_prefix = File.join(parent_mid, artifact_type)
    end

    output_subdir = File.join(@output_dir, artifact_prefix)
    FileUtils.mkdir_p(output_subdir)

    basename = case artifact_type
    when "annotated-slides"
      pres_name ? "#{sanitize_filename("annotated-#{File.basename(pres_name, File.extname(pres_name))}")}.pdf" : File.basename(source_path)
    else
      File.basename(source_path)
    end

    output_file = File.join(output_subdir, basename)

    if @dry_run
      @logger.info("[dry-run] Would copy #{source_path} -> #{output_file}")
    else
      FileUtils.cp(source_path, output_file)
    end
    @logger.info("Stored #{artifact_type}: #{output_file}")

    manifest = {
      "accessScope" => meeting_ctx["isBreakout"] ? "breakout" : "meeting",
      "artifactType" => artifact_type,
      "meetingId" => mid,
      "parentMeetingId" => parent_mid,
      "breakoutSequence" => meeting_ctx["sequence"],
      "authorizedUsers" => authorized_users,
      "mode" => @mode,
      "exportedAt" => Time.now.iso8601,
    }

    manifest_path = "#{output_file}.access.json"
    tmp = "#{manifest_path}.tmp"
    File.write(tmp, JSON.pretty_generate(manifest) + "\n")
    File.rename(tmp, manifest_path)
    @logger.info("Wrote access manifest: #{manifest_path}")

    { "artifact_type" => artifact_type, "meeting_id" => mid, "file" => output_file, "manifest" => manifest_path }
  end
end

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

BigBlueButton.logger.info("Phase 2 recording artifacts for [#{meeting_id}] starts")

begin
  recording_dir = BBB_PROPS["recording_dir"]
  events_xml = "#{recording_dir}/raw/#{meeting_id}/events.xml"

  unless File.exist?(events_xml)
    BigBlueButton.logger.info("No events.xml for #{meeting_id}, nothing to export")
    exit 0
  end

  meeting_metadata = BigBlueButton::Events.get_meeting_metadata(events_xml)
  config = load_env_config
  mode = detect_mode(meeting_metadata, config)

  BigBlueButton.logger.info("Detected mode=#{mode} for [#{meeting_id}]")

  exporter = RecordingArtifactsExporter.new(meeting_id, opts[:format], mode, config, BigBlueButton.logger)
  results = exporter.run

  slides = results.count { |r| r["artifact_type"] == "annotated-slides" }
  packages = results.count { |r| r["artifact_type"] == "raw-package" }
  BigBlueButton.logger.info("Phase 2 for [#{meeting_id}] completed: #{slides} slide(s), #{packages} package(s) (mode=#{mode})")

  # Copy per-meeting log to output directory so it's included in S3 uploads.
  # Flush the logger first to ensure all entries are written.
  logger.close rescue nil
  meeting_log = File.join(log_dir, "recording-artifacts-#{meeting_id}.log")
  if File.exist?(meeting_log)
    output_base = config["BBB_RECORDING_ARTIFACTS_OUTPUT_DIR"] || MODE_DEFAULTS[mode]["output_dir"]
    log_dest = File.join(output_base, meeting_id, "logs")
    FileUtils.mkdir_p(log_dest)
    FileUtils.cp(meeting_log, File.join(log_dest, "recording-artifacts.log"))
  end

  # Also copy post_archive log if it exists (Phase 1 snapshot failures)
  post_archive_log = File.join(log_dir, "post_archive.log")
  if File.exist?(post_archive_log)
    output_base = config["BBB_RECORDING_ARTIFACTS_OUTPUT_DIR"] || MODE_DEFAULTS[mode]["output_dir"]
    log_dest = File.join(output_base, meeting_id, "logs")
    FileUtils.mkdir_p(log_dest)
    FileUtils.cp(post_archive_log, File.join(log_dest, "post_archive.log"))
  end
rescue => e
  BigBlueButton.logger.warn("Phase 2 for [#{meeting_id}] failed: #{e.message}")
  BigBlueButton.logger.warn(e.backtrace.first(5).join("\n")) if e.backtrace
end

BigBlueButton.logger.info("Phase 2 recording artifacts for [#{meeting_id}] ends")
exit 0
