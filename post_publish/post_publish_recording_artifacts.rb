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
# Phase 2: post_publish hook — artifact generation & export
#
# Reads the Postgres snapshot from Phase 1 (artifacts-metadata.json) and
# generates annotated slide PDFs, access manifests, and rebuild sources.
#
# PDF rendering is delegated to bbb-export-annotations via its Redis queue.
# The job format matches the live application (StoreExportJobInRedisPresAnnEvent).
#
# For breakout rooms with "Capture Slides" enabled, pre-generated PDFs are
# used directly. Otherwise the breakout's own Phase 1 dump drives Redis
# generation — identical to parent meeting processing.
#
# S3 layout is designed for self-contained rebuilds: each meeting's export
# includes source SVGs and the annotation dump alongside generated PDFs.
#

require "optimist"
require "json"
require "fileutils"
require "redis"
require "aws-sdk-s3"
require "net/http"
require "jwt"
require "java_properties"
require "time"
require File.expand_path('../../../lib/recordandplayback', __FILE__)

CONFIG_FILE = "/etc/default/bbb-recording-artifacts"

BBB_PROPS = BigBlueButton.read_props

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
      line = line.sub(/\Aexport\s+/, "")
      key, value = line.split("=", 2)
      next unless key && value
      value = value.strip
      value = value[1..-2] if value.length >= 2 && value[0] == value[-1] && %w[" '].include?(value[0])
      config[key.strip] = value
    end
  end
  config
end

def get_metadata(key, meeting_metadata)
  meeting_metadata.key?(key) ? meeting_metadata[key].value : nil
end

METADATA_TO_CONFIG = {
  "artifactExportMode"              => "BBB_RECORDING_ARTIFACTS_MODE",
  "artifactExportS3Bucket"          => "BBB_RECORDING_ARTIFACTS_S3_BUCKET",
  "artifactExportS3Prefix"          => "BBB_RECORDING_ARTIFACTS_S3_PREFIX",
  "artifactExportS3Region"          => "BBB_RECORDING_ARTIFACTS_S3_REGION",
  "artifactExportAwsKeyId"          => "AWS_ACCESS_KEY_ID",
  "artifactExportAwsSecret"         => "AWS_SECRET_ACCESS_KEY",
  "artifactExportOutputDir"         => "BBB_RECORDING_ARTIFACTS_OUTPUT_DIR",
  "artifactExportIncludeBreakouts"  => "BBB_RECORDING_ARTIFACTS_INCLUDE_BREAKOUTS",
  "artifactExportCallbackUrl"       => "BBB_RECORDING_ARTIFACTS_CALLBACK_URL",
  "artifactExportNotes"             => "BBB_RECORDING_ARTIFACTS_EXPORT_NOTES",
  "artifactExportNotesFormats"      => "BBB_RECORDING_ARTIFACTS_NOTES_FORMATS",
  "artifactExportNotesFormat"       => "BBB_RECORDING_ARTIFACTS_NOTES_FORMAT",
}.freeze

def load_config(meeting_metadata)
  config = load_env_config
  METADATA_TO_CONFIG.each do |meta_key, config_key|
    value = get_metadata(meta_key, meeting_metadata)
    config[config_key] = value if value && !value.empty?
  end
  config
end

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

def parse_list(value, default)
  return default unless value && !value.empty?
  values = value.split(",").map { |item| item.strip.downcase }.reject(&:empty?)
  values.empty? ? default : values.uniq
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
    @wait_timeout   = parse_positive_integer(config["BBB_RECORDING_ARTIFACTS_WAIT_TIMEOUT"],  mode_defaults["wait_timeout"])
    @poll_interval  = parse_positive_integer(config["BBB_RECORDING_ARTIFACTS_POLL_INTERVAL"], mode_defaults["poll_interval"])
    @retry_max      = parse_positive_integer(config["BBB_RECORDING_ARTIFACTS_RETRY_MAX"],     mode_defaults["retry_max"])
    @retry_delay    = parse_positive_integer(config["BBB_RECORDING_ARTIFACTS_RETRY_DELAY"],   mode_defaults["retry_delay"])
    @include_breakouts = config["BBB_RECORDING_ARTIFACTS_INCLUDE_BREAKOUTS"] != "false"
    @dry_run           = config["BBB_RECORDING_ARTIFACTS_DRY_RUN"] == "true"
    @export_notes      = config["BBB_RECORDING_ARTIFACTS_EXPORT_NOTES"] == "true"
    @notes_formats     = parse_list(
      config["BBB_RECORDING_ARTIFACTS_NOTES_FORMATS"] || config["BBB_RECORDING_ARTIFACTS_NOTES_FORMAT"],
      ["pdf"]
    )

    @s3_bucket = config["BBB_RECORDING_ARTIFACTS_S3_BUCKET"]
    @s3_prefix = normalize_s3_prefix(config["BBB_RECORDING_ARTIFACTS_S3_PREFIX"])
    @s3_region = config["BBB_RECORDING_ARTIFACTS_S3_REGION"] || "us-east-1"
    @callback_url = config["BBB_RECORDING_ARTIFACTS_CALLBACK_URL"]
    @config = config

    @artifact_root = BBB_PROPS["raw_presentation_src"]
    @recording_dir = BBB_PROPS["recording_dir"]
    @published_dir = BBB_PROPS["published_dir"]
    @artifact_dir = File.join(@published_dir, "presentation", @meeting_id, "artifacts")
    @stage_root = File.join(
      config["BBB_RECORDING_ARTIFACTS_OUTPUT_DIR"] || mode_defaults["output_dir"],
      @meeting_id,
      ".stage"
    )
    @published_status_dir = File.join(@recording_dir, "status", "published")
    @redis_host = BBB_PROPS["redis_host"] || "127.0.0.1"
    @redis_port = BBB_PROPS["redis_port"] || 6379
    @redis_password = BBB_PROPS["redis_password"]

    FileUtils.mkdir_p(@artifact_dir)
    FileUtils.mkdir_p(@stage_root)
    FileUtils.mkdir_p(@published_status_dir)

    # expected_artifacts populated during export, read by build_access_manifest
    # and send_callback. Keys for breakouts are breakout meetingIds.
    @parent_expected_artifacts = []
    @breakout_expected_artifacts = {}

    @logger.level = mode_defaults["log_level"]
  end

  def run
    with_export_lock do
      if File.exist?(done_file)
        @logger.info("Artifact export already completed for #{@meeting_id}, skipping")
        next []
      end

      raw_dir = File.join(@recording_dir, "raw", @meeting_id)

      unless File.exist?(File.join(raw_dir, "events.xml"))
        @logger.info("No events.xml for #{@meeting_id}, skipping")
        next []
      end

      dump = load_phase1_dump(raw_dir)
      exports = []
      errors = []

      if dump
        @logger.info("Using Phase 1 Postgres dump for #{@meeting_id} (mode=#{@mode})")
        export(raw_dir, dump, exports, errors)
      else
        @logger.info("No Phase 1 dump for #{@meeting_id}, packaging raw files for external processing (mode=#{@mode})")
        package_raw_files(raw_dir, exports, errors)
      end

      if errors.empty?
        FileUtils.touch(done_file)
        FileUtils.rm_f(fail_file)
        FileUtils.rm_f(local_artifact_path("recording-artifacts.fail"))
        @logger.info("Artifact export complete for #{@meeting_id}: #{exports.length} artifact(s) (mode=#{@mode})")
        send_callback(exports) if exports.any?
      elsif exports.any?
        write_fail_file(errors)
        @logger.warn("Artifact export partial for #{@meeting_id}: #{exports.length} ok, #{errors.length} failed")
        send_callback(exports)
      else
        write_fail_file(errors)
        @logger.error("Artifact export failed for #{@meeting_id}: all artifacts failed")
      end

      exports
    end
  end

  def s3_configured?
    @s3_bucket && !@s3_bucket.empty? && @s3_prefix && !@s3_prefix.empty?
  end

  def upload_directory_to_s3(local_dir, remote_prefix)
    return [] unless s3_configured?

    uploaded = []
    Dir.glob(File.join(local_dir, "**", "*")).each do |file_path|
      next unless File.file?(file_path)
      relative = file_path.sub("#{local_dir}/", "")
      remote_key = "#{remote_prefix}/#{relative}"
      result = upload_to_s3(file_path, remote_key)
      uploaded << result if result
    end
    uploaded
  end

  def copy_and_upload_logs(log_dir, logger)
    logger.flush rescue nil

    log_dest = local_artifact_path("logs")
    FileUtils.mkdir_p(log_dest)

    {
      "recording-artifacts.log" => File.join(log_dir, "recording-artifacts-#{@meeting_id}.log"),
      "post_archive.log" => File.join(log_dir, "post_archive.log"),
      "post_publish.log" => File.join(log_dir, "post_publish.log"),
    }.each do |dest_name, source_path|
      next unless File.exist?(source_path)
      FileUtils.cp(source_path, File.join(log_dest, dest_name))
    end

    upload_directory_to_s3(log_dest, "#{@meeting_id}/logs") if File.directory?(log_dest)
  rescue => e
    @logger.warn("Log upload failed for #{@meeting_id}: #{e.message}")
  end

  def cleanup_stage
    FileUtils.rm_rf(@stage_root) if File.directory?(@stage_root)
  rescue => e
    @logger.warn("Stage cleanup failed for #{@meeting_id}: #{e.message}")
  end

  private

  # =========================================================================
  # Export orchestration
  # =========================================================================

  def export(raw_dir, dump, exports, errors)
    mid = @meeting_id

    # 1. Process parent meeting
    process_meeting(raw_dir, dump, mid, exports, errors)

    # 2. Process breakout rooms
    if @include_breakouts
      process_breakouts(dump, mid, exports, errors)
    end

    export_shared_notes(raw_dir, mid, exports, errors) if @export_notes

    # 3. Build access manifest (raw_dir needed so manifest can list the notes
    # files Phase 2 actually has on disk — not just what config requests).
    begin
      result = build_access_manifest(dump, raw_dir)
      exports << result if result
    rescue => e
      @logger.error("Access manifest failed for #{mid}: #{e.message}")
      errors << { "meeting_id" => mid, "scope" => "parent", "type" => "access-manifest", "error" => e.message }
    end
  end

  # =========================================================================
  # Unified meeting processor — works for both parent and breakout
  # =========================================================================
  #
  # Generates annotated PDFs for each presentation with annotations,
  # uploads the dump and source slides to S3 for rebuild capability.

  def process_meeting(raw_dir, dump, s3_subpath, exports, errors)
    mid = dump["meeting"]["meetingId"]
    presentations = dump["presentations"] || []
    scope = s3_subpath == @meeting_id ? "parent" : "breakout:#{mid}"

    if presentations.empty?
      @logger.warn("No presentations in Phase 1 dump for #{mid}")
    end

    # Upload artifacts-metadata.json
    begin
      result = store_json_artifact(raw_dir, "artifacts-metadata.json", s3_subpath, mid)
      exports << result if result
    rescue => e
      @logger.error("Artifacts metadata failed for #{mid}: #{e.message}")
      errors << { "meeting_id" => mid, "scope" => scope, "type" => "artifacts-metadata", "error" => e.message }
    end

    upload_all_sources(raw_dir, s3_subpath)

    # Generate and store annotated PDFs
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
        pdf_path = find_or_generate_pdf(mid, s3_subpath, pres, pres_location, pages)
        result = store_pdf_artifact(pdf_path, s3_subpath, mid, pres["name"])
        exports << result if result
      rescue => e
        @logger.error("Annotated PDF failed for #{pres_id} in #{mid}: #{e.message}")
        @logger.error(e.backtrace.first(3).join("\n")) if e.backtrace
        errors << { "meeting_id" => mid, "scope" => scope, "type" => "annotated-slides", "presId" => pres_id, "error" => e.message }
      end
    end
  end

  # =========================================================================
  # Breakout room orchestration
  # =========================================================================

  def process_breakouts(dump, parent_mid, exports, errors)
    (dump["breakouts"] || []).each do |breakout|
      br_mid = breakout["meetingId"]
      br_raw_dir = File.join(@recording_dir, "raw", br_mid)
      br_s3_subpath = "#{parent_mid}/breakouts/#{br_mid}"

      unless File.directory?(br_raw_dir)
        @logger.warn("Breakout raw dir missing for #{br_mid}, skipping")
        errors << { "meeting_id" => br_mid, "scope" => "breakout:#{br_mid}", "type" => "breakout-export", "error" => "raw dir missing" }
        next
      end

      # Fast path: pre-generated PDFs from captureslides
      captureslide_pdfs = find_captureslide_pdfs(br_raw_dir)
      unless captureslide_pdfs.empty?
        @logger.info("Found #{captureslide_pdfs.length} pre-generated PDF(s) for breakout #{br_mid}")
        begin
          result = store_json_artifact(br_raw_dir, "artifacts-metadata.json", br_s3_subpath, br_mid)
          exports << result if result
        rescue => e
          @logger.warn("Breakout metadata upload failed for #{br_mid}: #{e.message}")
        end
        upload_all_sources(br_raw_dir, br_s3_subpath)
        captureslide_pdfs.each do |pdf_path|
          result = store_pdf_artifact(pdf_path, br_s3_subpath, br_mid)
          exports << result if result
        end
        # Captureslide path uploads each PDF under its own basename
        # (store_pdf_artifact falls back to File.basename(source) when no pres_name).
        @breakout_expected_artifacts[br_mid] = captureslide_pdfs.map { |p| File.basename(p) }
        next
      end

      # Slow path: generate from breakout's own Phase 1 dump
      br_dump = load_phase1_dump(br_raw_dir)
      unless br_dump
        @logger.warn("No Phase 1 dump for breakout #{br_mid}, cannot generate annotations")
        errors << { "meeting_id" => br_mid, "scope" => "breakout:#{br_mid}", "type" => "breakout-export", "error" => "no Phase 1 dump" }
        next
      end

      @logger.info("No pre-generated PDFs for breakout #{br_mid}, generating via Redis from Phase 1 dump")
      process_meeting(br_raw_dir, br_dump, br_s3_subpath, exports, errors)
      @breakout_expected_artifacts[br_mid] = expected_annotated_pdfs(br_dump)
    end
  end

  # =========================================================================
  # Artifact storage
  # =========================================================================

  # Store a JSON file (dump or manifest) to output dir and S3.
  def store_json_artifact(source_dir, filename, s3_subpath, mid)
    src = File.join(source_dir, filename)
    unless File.exist?(src)
      @logger.warn("No #{filename} in #{source_dir}")
      return nil
    end

    output_subdir = local_artifact_path(local_subpath_for_s3_subpath(s3_subpath))
    FileUtils.mkdir_p(output_subdir)

    dest = File.join(output_subdir, filename)
    FileUtils.cp(src, dest)
    @logger.info("Stored #{filename}: #{dest}")

    remote_file = upload_to_s3(dest, "#{s3_subpath}/#{filename}")

    { "meeting_id" => mid, "file" => dest, "remote_file" => remote_file }
  end

  # Store a PDF artifact to output dir and S3.
  def store_pdf_artifact(source_path, s3_subpath, mid, pres_name = nil)
    output_subdir = local_artifact_path(local_subpath_for_s3_subpath(s3_subpath))
    FileUtils.mkdir_p(output_subdir)

    basename = if pres_name
      "#{sanitize_filename("annotated-#{File.basename(pres_name, File.extname(pres_name))}")}.pdf"
    else
      File.basename(source_path)
    end

    output_file = File.join(output_subdir, basename)

    if @dry_run
      @logger.info("[dry-run] Would copy #{source_path} -> #{output_file}")
    elsif File.expand_path(source_path) != File.expand_path(output_file)
      FileUtils.cp(source_path, output_file)
    end
    @logger.info("Stored artifact: #{output_file}")

    remote_file = upload_to_s3(@dry_run ? source_path : output_file, "#{s3_subpath}/#{basename}")

    { "meeting_id" => mid, "file" => output_file, "remote_file" => remote_file }
  end

  def store_notes_artifact(source_path, format, s3_subpath, mid)
    output_subdir = local_artifact_path(File.join(local_subpath_for_s3_subpath(s3_subpath), "shared-notes"))
    FileUtils.mkdir_p(output_subdir)

    basename = "notes.#{format}"
    output_file = File.join(output_subdir, basename)
    FileUtils.cp(source_path, output_file) unless @dry_run
    @logger.info("Stored shared notes artifact: #{output_file}")

    remote_file = upload_to_s3(@dry_run ? source_path : output_file, "#{s3_subpath}/shared-notes/#{basename}")

    { "meeting_id" => mid, "file" => output_file, "remote_file" => remote_file, "type" => "shared-notes", "format" => format }
  end

  # Upload source SVGs and original PDF to S3 for rebuild capability.
  # Non-fatal: failures are logged but don't block the export.
  def upload_sources(raw_dir, pres_id, s3_subpath)
    pres_dir = File.join(raw_dir, "presentation", pres_id)
    return unless File.directory?(pres_dir)

    local_sources_dir = local_artifact_path(File.join(local_subpath_for_s3_subpath(s3_subpath), "sources", pres_id))

    # SVGs
    svgs_dir = File.join(pres_dir, "svgs")
    if File.directory?(svgs_dir)
      local_svgs_dir = File.join(local_sources_dir, "svgs")
      FileUtils.mkdir_p(local_svgs_dir)
      Dir.glob(File.join(svgs_dir, "slide*.svg")).each do |svg|
        FileUtils.cp(svg, File.join(local_svgs_dir, File.basename(svg))) unless @dry_run
        upload_to_s3(svg, "#{s3_subpath}/sources/#{pres_id}/svgs/#{File.basename(svg)}")
      end
    end

    # Original PDF
    FileUtils.mkdir_p(local_sources_dir)
    Dir.glob(File.join(pres_dir, "*.pdf")).each do |pdf|
      FileUtils.cp(pdf, File.join(local_sources_dir, File.basename(pdf))) unless @dry_run
      upload_to_s3(pdf, "#{s3_subpath}/sources/#{pres_id}/#{File.basename(pdf)}")
    end
  rescue => e
    @logger.warn("Source upload failed for #{pres_id} (non-fatal): #{e.message}")
  end

  def upload_all_sources(raw_dir, s3_subpath)
    pres_root = File.join(raw_dir, "presentation")
    return unless File.directory?(pres_root)

    Dir.children(pres_root).sort.each do |pres_id|
      upload_sources(raw_dir, pres_id, s3_subpath)
    end
  end

  # =========================================================================
  # Access manifest
  # =========================================================================

  # v2 access manifest. Changes from v1:
  #   - version: 2
  #   - users[].extId → users[].ext_user_id (drop legacy field name)
  #   - users dropped: userId (internal), keep only ext_user_id/name/moderator
  #   - expected_artifacts at parent and per-breakout level for reconciliation
  # Meeting-level "extId" (the meeting's external id) is kept unchanged.
  def build_access_manifest(dump, raw_dir)
    meeting_ctx = dump["meeting"]
    mid = meeting_ctx["meetingId"]
    breakouts = dump["breakouts"] || []
    breakout_users = dump["breakoutUsers"] || {}

    @parent_expected_artifacts = expected_annotated_pdfs(dump) + expected_parent_notes(raw_dir)

    manifest = {
      "version" => 2,
      "meetingId" => mid,
      "extId" => meeting_ctx["extId"],
      "expected_artifacts" => @parent_expected_artifacts,
      "users" => (dump["users"] || []).map { |u| normalize_user(u) },
      "breakouts" => breakouts.map do |br|
        br_mid = br["meetingId"]
        {
          "meetingId" => br_mid,
          "sequence" => br["sequence"],
          "name" => br["name"],
          "expected_artifacts" => @breakout_expected_artifacts[br_mid] || [],
          "users" => (breakout_users[br_mid] || []).map { |u| normalize_user(u) },
        }
      end,
    }

    output_subdir = local_artifact_path(local_subpath_for_s3_subpath(mid))
    FileUtils.mkdir_p(output_subdir)

    manifest_path = File.join(output_subdir, "access-manifest.json")
    File.write(manifest_path, JSON.pretty_generate(manifest) + "\n")
    @logger.info("Stored access manifest: #{manifest_path}")

    remote_file = upload_to_s3(manifest_path, "#{mid}/access-manifest.json")

    { "meeting_id" => mid, "file" => manifest_path, "remote_file" => remote_file }
  end

  # Phase 1 dump shape → v2 manifest shape.
  def normalize_user(u)
    {
      "ext_user_id" => u["extId"],
      "name" => u["name"],
      "moderator" => u["isModerator"] == true,
    }
  end

  # =========================================================================
  # Raw file packaging (degraded mode — no Phase 1 dump)
  # =========================================================================

  def package_raw_files(raw_dir, exports, errors)
    mid = @meeting_id
    package_dir = local_artifact_path("raw-package")
    FileUtils.mkdir_p(package_dir)

    files_copied = 0

    events_src = File.join(raw_dir, "events.xml")
    if File.exist?(events_src)
      FileUtils.cp(events_src, File.join(package_dir, "events.xml"))
      files_copied += 1
      @logger.info("Packaged events.xml for #{mid}")
    else
      @logger.warn("No events.xml found in #{raw_dir}")
      errors << { "meeting_id" => mid, "scope" => "parent", "type" => "raw-package", "error" => "events.xml missing" }
    end

    metadata_src = File.join(raw_dir, "artifacts-metadata.json")
    if File.exist?(metadata_src)
      FileUtils.cp(metadata_src, File.join(package_dir, "artifacts-metadata.json"))
      files_copied += 1
    end

    metadata_fail_src = File.join(raw_dir, "artifacts-metadata.fail")
    if File.exist?(metadata_fail_src)
      FileUtils.cp(metadata_fail_src, File.join(package_dir, "artifacts-metadata.fail"))
      files_copied += 1
    end

    pres_src = File.join(raw_dir, "presentation")
    if File.directory?(pres_src)
      pres_dest = File.join(package_dir, "presentation")
      Dir.children(pres_src).each do |pres_id|
        pres_path = File.join(pres_src, pres_id)
        next unless File.directory?(pres_path)

        svgs_src = File.join(pres_path, "svgs")
        if File.directory?(svgs_src)
          svgs_dest = File.join(pres_dest, pres_id, "svgs")
          FileUtils.mkdir_p(svgs_dest)
          Dir.glob(File.join(svgs_src, "*.svg")).each do |svg|
            FileUtils.cp(svg, svgs_dest)
            files_copied += 1
          end
        end

        Dir.glob(File.join(pres_path, "*.pdf")).each do |pdf|
          pdf_dest = File.join(pres_dest, pres_id)
          FileUtils.mkdir_p(pdf_dest)
          FileUtils.cp(pdf, pdf_dest)
          files_copied += 1
        end

        pdfs_src = File.join(pres_path, "pdfs")
        if File.directory?(pdfs_src)
          pdfs_dest = File.join(pres_dest, pres_id, "pdfs")
          FileUtils.cp_r(pdfs_src, pdfs_dest)
          files_copied += 1
        end
      end
    end

    notes_src = File.join(raw_dir, "notes")
    if File.directory?(notes_src)
      notes_dest = File.join(package_dir, "notes")
      FileUtils.mkdir_p(notes_dest)
      Dir.glob(File.join(notes_src, "notes.*")).each do |note|
        next unless File.file?(note)
        FileUtils.cp(note, notes_dest)
        files_copied += 1
      end
    end

    if files_copied > 0
      remote_files = upload_directory_to_s3(package_dir, "#{mid}/raw-package")
      exports << { "meeting_id" => mid, "file" => package_dir, "remote_files" => remote_files }
      @logger.info("Packaged #{files_copied} raw file(s) for external processing: #{package_dir}")
      @logger.info("Uploaded #{remote_files.length} file(s) to S3") if remote_files.any?
    else
      @logger.warn("No raw files found to package for #{mid}")
      errors << { "meeting_id" => mid, "scope" => "parent", "type" => "raw-package", "error" => "no files found" }
    end
  end

  # =========================================================================
  # PDF generation
  # =========================================================================

  # Check for an existing artifact from a previous run, or generate a new one.
  def find_or_generate_pdf(mid, s3_subpath, pres, pres_location, pages)
    pres_name = pres["name"] || pres["presentationId"]
    basename = "#{sanitize_filename("annotated-#{File.basename(pres_name, File.extname(pres_name))}")}.pdf"
    candidate = local_artifact_path(File.join(local_subpath_for_s3_subpath(s3_subpath), basename))

    if File.exist?(candidate) && pdf_complete?(candidate)
      @logger.info("Artifact already exists locally: #{candidate}, skipping PDF generation")
      return candidate
    end

    export_input_dir = prepare_annotation_export_input(mid, pres["presentationId"], pres_name, pres_location)
    generate_annotated_pdf(mid, pres["presentationId"], pres_name, export_input_dir, pages)
  end

  # bbb-export-annotations expects presLocation to contain:
  #   {presId}.pdf
  #   svgs/slideN.svg
  # Raw archives often keep the original uploaded PDF filename instead, so stage
  # a normalized input directory before queueing the Redis job.
  def prepare_annotation_export_input(mid, pres_id, pres_name, source_dir)
    expected_pdf = File.join(source_dir, "#{pres_id}.pdf")
    svgs_dir = File.join(source_dir, "svgs")

    if File.file?(expected_pdf) && File.directory?(svgs_dir)
      @logger.info("Using annotation export input directly: #{source_dir}")
      return source_dir
    end

    pdf_source = find_presentation_pdf(source_dir, pres_id, pres_name)
    raise "No source PDF found for #{pres_id} in #{source_dir}" unless pdf_source
    raise "No SVG slides found for #{pres_id} in #{source_dir}" unless File.directory?(svgs_dir)

    stage_dir = File.join(@stage_root, "export-inputs", mid, pres_id)
    FileUtils.rm_rf(stage_dir)
    FileUtils.mkdir_p(stage_dir)
    FileUtils.cp(pdf_source, File.join(stage_dir, "#{pres_id}.pdf"))
    FileUtils.cp_r(svgs_dir, File.join(stage_dir, "svgs"))

    @logger.info("Prepared annotation export input for #{pres_id}: pdf=#{pdf_source}, svgs=#{svgs_dir}, stage=#{stage_dir}")
    stage_dir
  end

  def find_presentation_pdf(source_dir, pres_id, pres_name)
    candidates = [File.join(source_dir, "#{pres_id}.pdf"), File.join(source_dir, "#{pres_id}.PDF")]

    if pres_name && !pres_name.empty?
      basename = File.basename(pres_name, File.extname(pres_name))
      candidates << File.join(source_dir, pres_name)
      candidates << File.join(source_dir, "#{basename}.pdf")
      candidates << File.join(source_dir, "#{basename}.PDF")
    end

    candidates += Dir.glob(File.join(source_dir, "*.pdf"))
    candidates += Dir.glob(File.join(source_dir, "*.PDF"))
    candidates.find { |path| File.file?(path) }
  end

  # Push a job to bbb-export-annotations via Redis and wait for the output PDF.
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
      redis = redis_client
      begin
        redis.del(job_id)
        annotations_payload.each { |k, v| redis.hset(job_id, k, v) }
        redis.rpush("exportJobs", JSON.generate(export_job))
      ensure
        redis.close
      end
    end

    wait_for_pdf(output_path)

    begin
      redis = redis_client
      redis.del(job_id)
    rescue => e
      @logger.debug("Redis cleanup for #{job_id} failed (non-fatal): #{e.message}")
    ensure
      redis.close rescue nil
    end

    output_path
  end

  def wait_for_pdf(path)
    deadline = Time.now + @wait_timeout
    while Time.now < deadline
      if File.exist?(path) && File.size(path) > 0
        return path if pdf_complete?(path)
        @logger.debug("File #{path} exists but PDF not yet complete, waiting...")
      end
      sleep(@poll_interval)
    end
    raise "Timed out waiting for #{path} after #{@wait_timeout}s"
  end

  def pdf_complete?(path)
    size = File.size(path)
    return false if size < 32
    header = File.binread(path, 5)
    return false unless header == "%PDF-"
    tail = File.binread(path, [size, 32].min, [size - 32, 0].max)
    tail.include?("%%EOF")
  end

  # =========================================================================
  # Helpers
  # =========================================================================

  def resolve_pres_location(raw_dir, mid, pres_id)
    live = File.join(@artifact_root, mid, mid, pres_id)
    raw = File.join(raw_dir, "presentation", pres_id)
    if File.directory?(File.join(live, "svgs"))
      live
    elsif File.directory?(File.join(raw, "svgs"))
      raw
    end
  end

  def find_captureslide_pdfs(raw_dir)
    pres_dir = File.join(raw_dir, "presentation")
    return [] unless File.directory?(pres_dir)
    Dir.glob(File.join(pres_dir, "*", "pdfs", "**", "*.pdf")).select { |pdf| pdf_complete?(pdf) }
  end

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

  def export_shared_notes(raw_dir, s3_subpath, exports, errors)
    @notes_formats.each do |format|
      notes_file = find_notes_file(raw_dir, format)
      unless notes_file
        @logger.info("No non-empty shared notes file notes.#{format} for #{@meeting_id}, skipping")
        next
      end

      begin
        result = store_notes_artifact(notes_file, format, s3_subpath, @meeting_id)
        exports << result if result
      rescue => e
        @logger.error("Shared notes export failed for #{@meeting_id} format=#{format}: #{e.message}")
        errors << { "meeting_id" => @meeting_id, "scope" => "parent", "type" => "shared-notes", "format" => format, "error" => e.message }
      end
    end
  end

  def find_notes_file(raw_dir, format)
    candidates = [
      File.join(raw_dir, "notes", "notes.#{format}"),
      File.join(@published_dir, "notes", @meeting_id, "notes.#{format}"),
      File.join(@published_dir, "presentation", @meeting_id, "notes.#{format}"),
    ]
    candidates.find { |path| File.file?(path) && File.size(path).positive? }
  end

  # =========================================================================
  # Expected artifacts (manifest v2)
  # =========================================================================
  # Derived in Phase 2 because only Phase 2 sees the runtime config that
  # determines what will actually be uploaded (@notes_formats, @export_notes,
  # @include_breakouts) AND the on-disk state (captureslide PDFs, notes files).

  # Annotated PDFs Phase 2 will produce from a Phase 1 dump's annotation rows.
  # Filename must match store_pdf_artifact's basename derivation when pres_name
  # is set: sanitize_filename("annotated-{pres_name without extension}") + ".pdf".
  def expected_annotated_pdfs(dump)
    (dump["presentations"] || []).each_with_object([]) do |pres, acc|
      pages = pres["pages"] || []
      next unless pages.any? { |p| p["annotations"] && !p["annotations"].empty? }
      pres_name = pres["name"] || pres["presentationId"]
      base = File.basename(pres_name, File.extname(pres_name))
      acc << "#{sanitize_filename("annotated-#{base}")}.pdf"
    end
  end

  # Notes Phase 2 will upload for the parent. Skips formats whose source is
  # missing or empty (matches export_shared_notes / find_notes_file behavior).
  def expected_parent_notes(raw_dir)
    return [] unless @export_notes
    @notes_formats.each_with_object([]) do |fmt, acc|
      acc << "shared-notes/notes.#{fmt}" if find_notes_file(raw_dir, fmt)
    end
  end

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

  def redis_client
    opts = { host: @redis_host, port: @redis_port }
    opts[:password] = @redis_password if @redis_password && !@redis_password.empty?
    Redis.new(opts)
  end

  # --- S3 ---

  def s3_client
    @s3_client ||= begin
      credentials_opts = {}
      if @config["AWS_ACCESS_KEY_ID"] && @config["AWS_SECRET_ACCESS_KEY"]
        credentials_opts[:credentials] = Aws::Credentials.new(
          @config["AWS_ACCESS_KEY_ID"],
          @config["AWS_SECRET_ACCESS_KEY"]
        )
      end
      Aws::S3::Client.new(region: @s3_region, **credentials_opts)
    end
  end

  def upload_to_s3(local_path, remote_key)
    return nil unless s3_configured?

    full_key = s3_key(remote_key)

    if @dry_run
      @logger.info("[dry-run] Would upload #{local_path} -> s3://#{@s3_bucket}/#{full_key}")
      return "s3://#{@s3_bucket}/#{full_key}"
    end

    local_size = File.size(local_path)
    begin
      head = s3_client.head_object(bucket: @s3_bucket, key: full_key)
      if head.content_length == local_size
        @logger.info("Already on S3 (size match): s3://#{@s3_bucket}/#{full_key}")
        return "s3://#{@s3_bucket}/#{full_key}"
      end
      @logger.info("S3 object exists but size differs (local=#{local_size}, remote=#{head.content_length}), re-uploading")
    rescue Aws::S3::Errors::NotFound
      # Object doesn't exist yet, proceed with upload
    end

    with_retries("S3 upload #{File.basename(local_path)}") do
      File.open(local_path, "rb") do |file|
        s3_client.put_object(bucket: @s3_bucket, key: full_key, body: file)
      end
    end
    @logger.info("Uploaded #{local_path} -> s3://#{@s3_bucket}/#{full_key}")
    "s3://#{@s3_bucket}/#{full_key}"
  end

  # --- Callback ---

  def send_callback(exports)
    return unless @callback_url && !@callback_url.empty?

    bbb_web_properties = "/etc/bigbluebutton/bbb-web.properties"
    unless File.exist?(bbb_web_properties)
      @logger.warn("Cannot send callback: #{bbb_web_properties} not found")
      return
    end

    props = JavaProperties::Properties.new(bbb_web_properties)
    secret = props[:securitySalt]
    unless secret
      @logger.warn("Cannot send callback: securitySalt not found in bbb-web.properties")
      return
    end

    artifacts = exports.map do |e|
      entry = { "meeting_id" => e["meeting_id"], "file" => e["file"] }
      entry["remote_file"] = e["remote_file"] if e["remote_file"]
      entry["remote_files"] = e["remote_files"] if e["remote_files"]
      entry
    end

    # Mirror the manifest's expected_artifacts so Django can reconcile uploads
    # from the callback alone — no extra S3 read. Same data the manifest holds.
    breakouts_expected = @breakout_expected_artifacts.map do |br_mid, expected|
      { "meeting_id" => br_mid, "expected_artifacts" => expected }
    end

    payload = {
      meeting_id: @meeting_id,
      artifacts: artifacts,
      expected_artifacts: @parent_expected_artifacts,
      breakouts: breakouts_expected,
    }
    payload_encoded = JWT.encode(payload, secret)

    if @dry_run
      @logger.info("[dry-run] Would POST callback to #{@callback_url}")
      return
    end

    with_retries("Callback POST") do
      uri = URI.parse(@callback_url)
      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl = (uri.scheme == "https")
      http.open_timeout = 10
      http.read_timeout = 30

      request = Net::HTTP::Post.new(uri.request_uri)
      request.set_form_data({ signed_parameters: payload_encoded })

      response = http.request(request)
      code = response.code.to_i

      if code >= 200 && code < 300
        @logger.info("Callback successful for #{@meeting_id} (code #{code})")
      elsif code == 410
        @logger.info("Callback returned 410 (gone) for #{@meeting_id}")
      else
        raise "Callback HTTP #{code} #{response.message}"
      end
    end
  rescue => e
    @logger.warn("Callback failed for #{@meeting_id}: #{e.message}")
  end

  # --- Status/lock files ---

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
    payload = {
      "meeting_id" => @meeting_id, "mode" => @mode,
      "timestamp" => Time.now.iso8601, "errors" => errors,
    }
    content = JSON.pretty_generate(payload) + "\n"
    File.write(fail_file, content)

    artifact_fail = local_artifact_path("recording-artifacts.fail")
    FileUtils.mkdir_p(File.dirname(artifact_fail))
    File.write(artifact_fail, content)
    upload_to_s3(artifact_fail, "#{@meeting_id}/recording-artifacts.fail")
  rescue => e
    @logger.warn("Could not write or upload artifact fail file for #{@meeting_id}: #{e.message}")
  end

  def with_export_lock
    File.open(lock_file, File::RDWR | File::CREAT, 0o644) do |f|
      f.flock(File::LOCK_EX)
      yield
    ensure
      f.flock(File::LOCK_UN) rescue nil
    end
  end

  def local_subpath_for_s3_subpath(s3_subpath)
    return "" if s3_subpath == @meeting_id
    prefix = "#{@meeting_id}/"
    s3_subpath.start_with?(prefix) ? s3_subpath.delete_prefix(prefix) : s3_subpath
  end

  def local_artifact_path(relative_path)
    relative_path.nil? || relative_path.empty? ? @artifact_dir : File.join(@artifact_dir, relative_path)
  end

  def normalize_s3_prefix(prefix)
    return nil unless prefix
    prefix.strip.gsub(%r{\A/+|/+\z}, "")
  end

  def s3_key(remote_key)
    "#{@s3_prefix}/#{remote_key.gsub(%r{\A/+}, "")}"
  end
end

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

BigBlueButton.logger.info("Phase 2 recording artifacts for [#{meeting_id}] starts")

config = load_env_config
mode = detect_mode({}, config)
exporter = nil

begin
  recording_dir = BBB_PROPS["recording_dir"]
  events_xml = "#{recording_dir}/raw/#{meeting_id}/events.xml"

  unless File.exist?(events_xml)
    BigBlueButton.logger.info("No events.xml for #{meeting_id}, nothing to export")
    exit 0
  end

  # Breakout rooms are processed by the parent meeting's export.
  begin
    events_doc = Nokogiri::XML(File.open(events_xml))
    meeting_el = events_doc.at_xpath("/recording/meeting")
    if meeting_el && meeting_el["breakout"] == "true"
      BigBlueButton.logger.info("#{meeting_id} is a breakout room, skipping (handled by parent export)")
      exit 0
    end
  rescue => e
    BigBlueButton.logger.warn("Could not parse events.xml for breakout check: #{e.message}, continuing")
  end

  meeting_metadata = BigBlueButton::Events.get_meeting_metadata(events_xml)
  config = load_config(meeting_metadata)
  mode = detect_mode(meeting_metadata, config)

  BigBlueButton.logger.info("Detected mode=#{mode} for [#{meeting_id}]")

  exporter = RecordingArtifactsExporter.new(meeting_id, opts[:format], mode, config, BigBlueButton.logger)
  results = exporter.run

  BigBlueButton.logger.info("Phase 2 for [#{meeting_id}] completed: #{results.length} artifact(s) (mode=#{mode})")
rescue => e
  BigBlueButton.logger.warn("Phase 2 for [#{meeting_id}] failed: #{e.message}")
  BigBlueButton.logger.warn(e.backtrace.first(5).join("\n")) if e.backtrace
end

BigBlueButton.logger.info("Phase 2 recording artifacts for [#{meeting_id}] ends")

# Copy logs to output directory and upload to S3.
begin
  exporter.copy_and_upload_logs(log_dir, logger) if exporter
rescue => e
  $stderr.puts("Log copy failed for #{meeting_id}: #{e.message}")
ensure
  exporter.cleanup_stage if exporter
end

logger.close rescue nil
exit 0
