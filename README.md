# bbb-recording-artifacts

Two-phase post-hook system that exports annotated slide PDFs and access manifests from BigBlueButton recordings. Designed so that everything needed to rebuild annotations is stored on S3.

## How it works

**Phase 1 (post_archive)** runs after the recording has archived and passed sanity, while Postgres should still have meeting data. It snapshots presentations, annotations, users, and breakout room assignments into `artifacts-metadata.json` in the raw archive. This is critical because Postgres purges meeting data ~60 minutes after the meeting ends.

**Phase 2 (post_publish)** runs after a recording format is published. It reads the Phase 1 dump, generates annotated slide PDFs via bbb-export-annotations, copies final local artifacts under the published presentation recording, uploads everything to S3, and sends an optional callback notification. Breakout rooms are processed by the parent meeting's export.

```
Postgres (live, ephemeral)
  --> Phase 1: snapshot to artifacts-metadata.json (durable)
      --> Phase 2: generate PDFs, upload to S3
          --> canonical local artifacts + S3: annotated PDFs + source slides + dump + logs
```

Final local artifacts are stored under:

```text
/var/bigbluebutton/published/{format}/{meetingId}/artifacts/
```

`{format}` defaults to `presentation` and can be overridden with
`BBB_RECORDING_ARTIFACTS_LOCAL_FORMAT` (server-wide) or `meta_artifactExportLocalFormat`
(per-meeting). Deployments that disable the presentation publish format
should set this to a format that is published (e.g. `video`) so the local
artifact copy lives under a directory BBB's file-backed recording lifecycle
will actually manage.

This keeps local artifacts aligned with BBB's file-backed recording lifecycle. A Web API `deleteRecordings` soft-delete moves the published recording, including `artifacts/`, into `/var/bigbluebutton/deleted/{format}/{meetingId}/`. S3 retention and deletion remain owned by the downstream consumer.

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
sudo cp .env.example /etc/default/bbb-recording-artifacts
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

These gems are **not** included in a stock BBB install. Install the system libraries and gems into the recording pipeline's vendor bundle:

```bash
# Ruby headers (required to build bigdecimal and other native extensions)
# and libpq (required to build the pg gem native extension)
sudo apt-get install -y ruby3.0-dev libpq-dev

# Add the gems to the BBB recording bundle
cd /usr/local/bigbluebutton/core

bundle add pg --version '~> 1.4.0'
bundle add aws-sdk-s3 --version '~> 1.218'

bundle config set path 'vendor/bundle'
bundle install
```

**Note:** BBB 3.0 ships Ruby 3.0.2. The `pg` gem must be pinned to `~> 1.4.0` — versions 1.5+ require Ruby 3.1+ and will fail to install.

**Verify**:

```bash
sudo -u bigbluebutton bundle exec ruby -e 'require "pg"; require "aws-sdk-s3"; puts "ok"'
```

**Restart recording and playback processes**:

```bash
sudo systemctl restart bbb-rap-starter
sudo systemctl restart bbb-rap-resque-worker
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
| `BBB_RECORDING_ARTIFACTS_OUTPUT_DIR` | `artifactExportOutputDir` | mode-dependent | Temporary staging dir |
| `BBB_RECORDING_ARTIFACTS_LOCAL_FORMAT` | `artifactExportLocalFormat` | `presentation` | Format dir under `published_dir` where local artifacts are written |
| `BBB_RECORDING_ARTIFACTS_INCLUDE_BREAKOUTS` | `artifactExportIncludeBreakouts` | `true` | Process breakout rooms |
| `BBB_RECORDING_ARTIFACTS_CALLBACK_URL` | `artifactExportCallbackUrl` | _(none)_ | JWT-signed POST on completion |
| `BBB_RECORDING_ARTIFACTS_EXPORT_NOTES` | `artifactExportNotes` | `false` | Export archived shared notes |
| `BBB_RECORDING_ARTIFACTS_NOTES_FORMATS` | `artifactExportNotesFormats` | `pdf` | Comma-separated notes formats |
| `BBB_RECORDING_ARTIFACTS_WAIT_TIMEOUT` | | `180` (prod) | Seconds to wait for PDF |
| `BBB_RECORDING_ARTIFACTS_POLL_INTERVAL` | | `2` (prod) | Seconds between PDF polls |
| `BBB_RECORDING_ARTIFACTS_RETRY_MAX` | | `3` (prod) | Max retry attempts |
| `BBB_RECORDING_ARTIFACTS_RETRY_DELAY` | | `2` (prod) | Base retry delay (exponential backoff) |
| `BBB_RECORDING_ARTIFACTS_DRY_RUN` | | `false` | Log actions without writing |

### Mode defaults

| Setting | Dev | Prod |
|---------|-----|------|
| Staging dir | `recording-artifacts-dev/` | `recording-artifacts/` |
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
    access-manifest.json               # v3: users, breakouts, expected_artifacts (all snake_case)
    recording-artifacts.fail           # present only on partial/full failure
    annotated-{presentationName}.pdf   # annotated slides
    shared-notes/
        notes.pdf
        notes.html
        notes.etherpad
    sources/                           # rebuild sources
        {presId}/
            svgs/slide1.svg ... slideN.svg
            {original-filename}.pdf
    breakouts/
        {breakoutMeetingId}/
            artifacts-metadata.json    # breakout's own dump
            annotated-{name}.pdf
            shared-notes/              # breakouts have their own shared notes
                notes.pdf
                notes.html
            sources/
                {presId}/
                    svgs/slide1.svg ...
                    {original-filename}.pdf
    logs/
        recording-artifacts.log
        post_archive.log
        post_publish.log
```

S3 objects are not deleted by the BBB hook. Downstream systems should apply lifecycle rules or scheduled cleanup to `{prefix}/{parentMeetingId}/`.

## Shared notes

BBB archives shared notes during the archive step when notes were used and the text export is non-empty. The formats available to this exporter are the formats configured in `notes_formats` in `bigbluebutton.yml`. BBB commonly archives:

```yaml
notes_formats:
  - etherpad
  - html
  - pdf
```

`txt`, `doc`, and `odt` may also be available if the BBB deployment archives them.

Enable notes export server-wide:

```bash
BBB_RECORDING_ARTIFACTS_EXPORT_NOTES=true
BBB_RECORDING_ARTIFACTS_NOTES_FORMATS=pdf,html,etherpad
```

Or per-meeting on the BBB `create` API call:

```
meta_artifactExportNotes=true
meta_artifactExportNotesFormats=pdf,html,etherpad
```

The exporter copies only matching non-empty `notes.{format}` files that already exist in the raw archive or published notes output. Missing configured formats are logged and skipped; they do not mark the artifact export as failed.

**Breakouts:** when notes export is enabled, each breakout room's own shared notes are also exported from the breakout's raw archive to `{prefix}/{parentMeetingId}/breakouts/{breakoutMeetingId}/shared-notes/`. The same `notes_formats` setting applies to both parent and breakout notes.

## Breakout room handling

Breakout rooms are always processed by the parent meeting's Phase 2. When Phase 2 runs for a breakout room directly, it exits early.

Two strategies for breakout annotations:

1. **Capture Slides enabled** (fast path): The breakout room renders its own annotated PDF before closing. These pre-generated PDFs are found in the raw archive and copied directly to S3.

2. **Capture Slides disabled** (slow path): The breakout's own Phase 1 dump is loaded and PDFs are generated via bbb-export-annotations, identical to parent meeting processing.

In both paths, the breakout's shared notes (if enabled via `BBB_RECORDING_ARTIFACTS_EXPORT_NOTES` or `meta_artifactExportNotes=true`) are exported from the breakout's own raw archive and uploaded under `{prefix}/{parentMeetingId}/breakouts/{breakoutMeetingId}/shared-notes/`.

## Rebuilding from S3

The S3 export contains everything needed to regenerate annotated PDFs:

1. `artifacts-metadata.json` — annotations and page dimensions
2. `sources/{presId}/svgs/` — slide background SVGs
3. `sources/{presId}/{name}.pdf` — original uploaded PDF

To rebuild, download these files and either:
- Push a Redis job to bbb-export-annotations (same as Phase 2 does)
- Use the standalone `export_recording_artifacts_eventsxml.rb` tool to reconstruct the dump from `events.xml` if the original dump is unavailable

If Phase 1 fails, it writes `artifacts-metadata.fail` in the raw archive. When Phase 2 falls back to raw-package mode, that marker is included in `{meetingId}/raw-package/artifacts-metadata.fail` on S3.

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
    {"meeting_id": "abc123-...", "file": "/local/path/annotated-deck.pdf",
     "remote_file": "s3://...", "type": "annotated-slides"},
    {"meeting_id": "abc123-...", "file": "/local/path/notes.pdf",
     "remote_file": "s3://...", "type": "shared-notes", "format": "pdf"},
    {"meeting_id": "abc123-...", "file": "/local/path/access-manifest.json",
     "remote_file": "s3://...", "type": "access-manifest"},
    {"meeting_id": "br-...", "file": "/local/path/breakouts/br-.../annotated-deck.pdf",
     "remote_file": "s3://...", "type": "annotated-slides"}
  ],
  "expected_artifacts": ["annotated-deck.pdf", "shared-notes/notes.pdf"],
  "breakouts": [
    {"meeting_id": "br-...", "expected_artifacts": ["annotated-deck.pdf", "shared-notes/notes.pdf"]}
  ]
}
```

Each artifact carries its own `meeting_id` — for breakout artifacts this is
the breakout's internal id, not the parent's. Consumers should route artifacts
by `artifact.meeting_id`, not by the top-level `meeting_id`. The top-level
`meeting_id` is always the parent (the callback only fires once per parent
meeting, after all breakouts have been processed).

Possible `type` values: `annotated-slides`, `shared-notes` (also carries
`format`), `access-manifest`, `artifacts-metadata`, `raw-package`. The field
may be absent on entries produced by older runs.

`expected_artifacts` (paths relative to each meeting's S3 prefix) lets the
consumer reconcile what was actually uploaded against what Phase 2 intended
to upload — useful for detecting partial failures. Every breakout from the
Phase 1 dump appears in `breakouts[]`, including breakouts whose Phase 2
failed (their `expected_artifacts` will be empty). The list mirrors what
`access-manifest.json` carries.

Signed with `securitySalt` from `/etc/bigbluebutton/bbb-web.properties`.

## Logs

- Phase 1: `/var/log/bigbluebutton/post_archive.log`
- Phase 2 (per meeting): `/var/log/bigbluebutton/recording-artifacts-{meetingId}.log`
- Logs are also copied to `/var/bigbluebutton/published/presentation/{meetingId}/artifacts/logs/` and uploaded to S3 under `{meetingId}/logs/`

On partial or full artifact failure, `recording-artifacts.fail` is written locally under `artifacts/` and uploaded to S3 at `{meetingId}/recording-artifacts.fail`.

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

**Shared notes missing**: Verify the meeting actually used shared notes and that the requested formats exist in `/var/bigbluebutton/recording/raw/{meetingId}/notes/`. The exporter does not call Etherpad during post_publish; it only copies archived or published notes files.

## Retention and cleanup

Local artifacts are placed under the published presentation recording so BBB's normal file-backed soft-delete moves them with the recording. BBB's daily cleanup in this repository does not appear to purge `/var/bigbluebutton/deleted/` by age; operators who need local hard retention should configure a separate cron outside these hooks.

S3 cleanup is intentionally out of scope for BBB. The downstream consumer should own junior/senior retention, lifecycle rules, and deletion for each `{prefix}/{meetingId}/` tree.

## Files

| File | Purpose |
|------|---------|
| `post_archive/post_archive_recording_artifacts.rb` | Phase 1: Postgres snapshot |
| `post_publish/post_publish_recording_artifacts.rb` | Phase 2: PDF generation, S3 export |
| `post_publish/export_recording_artifacts_eventsxml.rb` | Standalone: reconstruct dump from events.xml |
| `.env.example` | Config template (install to `/etc/default/bbb-recording-artifacts`) |
