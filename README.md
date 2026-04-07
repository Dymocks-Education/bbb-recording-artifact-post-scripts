# bbb-recording-artifacts

Two-phase post-hook system that exports annotated slide PDFs and access manifests from BigBlueButton recordings. Designed so that everything needed to rebuild annotations is stored on S3.

## How it works

**Phase 1 (post_archive)** runs immediately after the recording archive step, while Postgres still has meeting data. It snapshots presentations, annotations, users, and breakout room assignments into `artifacts-metadata.json` in the raw archive. This is critical because Postgres purges meeting data ~60 minutes after the meeting ends.

**Phase 2 (post_publish)** runs after a recording format is published. It reads the Phase 1 dump, generates annotated slide PDFs via bbb-export-annotations, builds access manifests, uploads everything to S3, and sends an optional callback notification. Breakout rooms are processed by the parent meeting's export.

```
Postgres (live, ephemeral)
  --> Phase 1: snapshot to artifacts-metadata.json (durable)
      --> Phase 2: generate PDFs, upload to S3
          --> S3: annotated PDFs + source slides + dump (self-contained rebuild)
```

## Installation

### 1. Copy scripts

```bash
sudo cp post_archive/post_archive_recording_artifacts.rb \
  /usr/local/bigbluebutton/core/scripts/post_archive/

sudo cp post_publish/post_publish_recording_artifacts.rb \
  /usr/local/bigbluebutton/core/scripts/post_publish/
```

### 2. Install config

```bash
sudo cp post_publish/bbb-recording-artifacts.conf /etc/default/bbb-recording-artifacts
```

Edit `/etc/default/bbb-recording-artifacts` with your S3 credentials:

```bash
BBB_RECORDING_ARTIFACTS_MODE=prod
BBB_RECORDING_ARTIFACTS_S3_BUCKET=your-bucket-name
BBB_RECORDING_ARTIFACTS_S3_PREFIX=recording-artifacts
BBB_RECORDING_ARTIFACTS_S3_REGION=us-east-1
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
```

### 3. Install dependencies

These gems are **not** included in a stock BBB install. They must be added to the recording pipeline's Gemfile and installed into its vendor bundle.

**Add to Gemfile** (`/usr/local/bigbluebutton/core/Gemfile`):

```ruby
gem 'pg', '~> 1.4.0'
gem 'aws-sdk-s3', '~> 1.218'
```

**Note:** BBB 3.0 ships Ruby 3.0.2. The `pg` gem version must be pinned to `~> 1.4.0` — versions 1.5+ require Ruby 3.1+. Using `gem 'pg'` without a version constraint will pull a version that fails to install.

**Install the pg system library** (required to build the `pg` gem native extension):

```bash
sudo apt-get install -y libpq-dev
```

**Install gems into the vendor bundle**:

```bash
cd /usr/local/bigbluebutton/core
sudo bundle install --path vendor/bundle
```

**Verify**:

```bash
ls /usr/local/bigbluebutton/core/vendor/bundle/ruby/*/gems/ | grep -E 'pg-|aws-sdk-s3'
```

### 4. Verify bbb-export-annotations is running

Phase 2 delegates PDF rendering to bbb-export-annotations via Redis:

```bash
systemctl status bbb-export-annotations
```

## Configuration

All settings can be overridden at three levels (each overrides the previous):

| Level | Source | Example |
|-------|--------|---------|
| Defaults | Hardcoded per mode (dev/prod) | 180s timeout, 3 retries |
| Server | `/etc/default/bbb-recording-artifacts` | S3 bucket, credentials |
| Per-meeting | BBB API `meta_` params on create | `meta_artifactExportS3Bucket=tenant-bucket` |

### Available settings

| Config key | Metadata key | Default | Description |
|------------|-------------|---------|-------------|
| `BBB_RECORDING_ARTIFACTS_MODE` | `artifactExportMode` | `prod` | `dev` or `prod` |
| `BBB_RECORDING_ARTIFACTS_S3_BUCKET` | `artifactExportS3Bucket` | _(none)_ | S3 bucket name |
| `BBB_RECORDING_ARTIFACTS_S3_PREFIX` | `artifactExportS3Prefix` | _(none)_ | S3 key prefix |
| `BBB_RECORDING_ARTIFACTS_S3_REGION` | `artifactExportS3Region` | `us-east-1` | AWS region |
| `AWS_ACCESS_KEY_ID` | `artifactExportAwsKeyId` | _(none)_ | AWS credentials |
| `AWS_SECRET_ACCESS_KEY` | `artifactExportAwsSecret` | _(none)_ | AWS credentials |
| `BBB_RECORDING_ARTIFACTS_OUTPUT_DIR` | `artifactExportOutputDir` | mode-dependent | Local output dir |
| `BBB_RECORDING_ARTIFACTS_INCLUDE_BREAKOUTS` | `artifactExportIncludeBreakouts` | `true` | Process breakout rooms |
| `BBB_RECORDING_ARTIFACTS_CALLBACK_URL` | `artifactExportCallbackUrl` | _(none)_ | JWT-signed POST on completion |
| `BBB_RECORDING_ARTIFACTS_WAIT_TIMEOUT` | | `180` (prod) | Seconds to wait for PDF |
| `BBB_RECORDING_ARTIFACTS_POLL_INTERVAL` | | `2` (prod) | Seconds between PDF polls |
| `BBB_RECORDING_ARTIFACTS_RETRY_MAX` | | `3` (prod) | Max retry attempts |
| `BBB_RECORDING_ARTIFACTS_RETRY_DELAY` | | `2` (prod) | Base retry delay (exponential backoff) |
| `BBB_RECORDING_ARTIFACTS_DRY_RUN` | | `false` | Log actions without writing |

### Mode defaults

| Setting | Dev | Prod |
|---------|-----|------|
| Output dir | `recording-artifacts-dev/` | `recording-artifacts/` |
| Wait timeout | 120s | 180s |
| Poll interval | 1s | 2s |
| Max retries | 1 | 3 |
| Retry delay | 1s | 2s |
| Log level | DEBUG | INFO |

## S3 layout

Everything nests under the parent meeting ID. Each meeting's export is self-contained for rebuilds.

```
{prefix}/{parentMeetingId}/
    artifacts-metadata.json            # Phase 1 Postgres dump
    access-manifest.json               # user-to-breakout mapping
    annotated-{presentationName}.pdf   # annotated slides
    sources/                           # rebuild sources
        {presId}/
            svgs/slide1.svg ... slideN.svg
            {original-filename}.pdf
    breakouts/
        {breakoutMeetingId}/
            artifacts-metadata.json    # breakout's own dump
            annotated-{name}.pdf
            sources/
                {presId}/
                    svgs/slide1.svg ...
                    {original-filename}.pdf
    logs/
        recording-artifacts.log
        post_archive.log
```

## Breakout room handling

Breakout rooms are always processed by the parent meeting's Phase 2. When Phase 2 runs for a breakout room directly, it exits early.

Two strategies for breakout annotations:

1. **Capture Slides enabled** (fast path): The breakout room renders its own annotated PDF before closing. These pre-generated PDFs are found in the raw archive and copied directly to S3.

2. **Capture Slides disabled** (slow path): The breakout's own Phase 1 dump is loaded and PDFs are generated via bbb-export-annotations, identical to parent meeting processing.

## Rebuilding from S3

The S3 export contains everything needed to regenerate annotated PDFs:

1. `artifacts-metadata.json` — annotations and page dimensions
2. `sources/{presId}/svgs/` — slide background SVGs
3. `sources/{presId}/{name}.pdf` — original uploaded PDF

To rebuild, download these files and either:
- Push a Redis job to bbb-export-annotations (same as Phase 2 does)
- Use the standalone `export_recording_artifacts_eventsxml.rb` tool to reconstruct the dump from `events.xml` if the original dump is unavailable

## Callback notification

If `BBB_RECORDING_ARTIFACTS_CALLBACK_URL` is configured, Phase 2 sends a JWT-signed POST after export:

```
POST {callback_url}
Content-Type: application/x-www-form-urlencoded

signed_parameters={JWT_TOKEN}
```

The JWT payload contains:
```json
{
  "meeting_id": "abc123-...",
  "artifacts": [
    {"meeting_id": "...", "file": "/local/path", "remote_file": "s3://..."}
  ]
}
```

Signed with `securitySalt` from `/etc/bigbluebutton/bbb-web.properties`.

## Logs

- Phase 1: `/var/log/bigbluebutton/post_archive.log`
- Phase 2 (per meeting): `/var/log/bigbluebutton/recording-artifacts-{meetingId}.log`
- Logs are also copied to the output directory and uploaded to S3 under `{meetingId}/logs/`

## Concurrency and idempotency

- **Lock file**: Prevents duplicate exports when multiple post_publish hooks run concurrently (one per published format).
- **Done file**: Prevents re-export on subsequent format publishes or manual re-runs.
- **S3 dedup**: Skips upload if object already exists with matching size.
- **Safe re-runs**: Delete the `.done` file to force re-export:
  ```bash
  rm /var/bigbluebutton/recording/status/published/{meetingId}-recording-artifacts.done
  ```

## Troubleshooting

**Phase 1 dump missing**: Check `/var/log/bigbluebutton/post_archive.log`. Common cause: Postgres data expired before post_archive ran. Phase 2 falls back to packaging raw files for external processing.

**PDF generation timeout**: Check that bbb-export-annotations is running (`systemctl status bbb-export-annotations`). Increase `BBB_RECORDING_ARTIFACTS_WAIT_TIMEOUT` if PDFs are large.

**S3 upload fails**: Check credentials in `/etc/default/bbb-recording-artifacts`. Verify IAM permissions include `s3:PutObject` and `s3:HeadObject` on the target prefix.

**Breakout annotations missing**: Verify the breakout's raw archive exists at `/var/bigbluebutton/recording/raw/{breakoutId}/` and contains `artifacts-metadata.json`. If the breakout's post_archive failed, annotations cannot be generated.

## Files

| File | Purpose |
|------|---------|
| `post_archive/post_archive_recording_artifacts.rb` | Phase 1: Postgres snapshot |
| `post_publish/post_publish_recording_artifacts.rb` | Phase 2: PDF generation, S3 export |
| `post_publish/export_recording_artifacts_eventsxml.rb` | Standalone: reconstruct dump from events.xml |
| `post_publish/bbb-recording-artifacts.conf` | Config template (install to `/etc/default/`) |
