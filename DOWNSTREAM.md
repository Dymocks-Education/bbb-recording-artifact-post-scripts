# Downstream consumer contract

This is the contract the BBB recording artifact pipeline emits. It is what
the Django/Python (or any other downstream) consumer can rely on.

There are two surfaces:

1. **Callback** — JWT-signed POST sent once per parent meeting, immediately
   after Phase 2 finishes. Synchronous signal that artifacts are ready.
2. **Access manifest** — `access-manifest.json` written to S3 (and locally
   alongside the recording). Authoritative record of who/what is in the
   meeting and which artifacts to expect.

The callback announces "things just landed"; the manifest describes "what
those things are". Consumers typically read both — the callback drives the
processing pipeline, the manifest provides identity/reconciliation data.

## S3 layout

Everything for one logical meeting nests under the parent meeting's internal
id, regardless of breakouts:

```
{prefix}/{parent_meeting_id}/
    artifacts-metadata.json            # Phase 1 Postgres dump (internal shape)
    access-manifest.json               # v3 — see below
    recording-artifacts.fail           # present only on partial/full failure
    annotated-{presentationName}.pdf
    shared-notes/
        notes.pdf
        notes.html
        notes.etherpad
    sources/{presId}/
        svgs/slide1.svg ... slideN.svg
        {original-filename}.pdf
    breakouts/{breakout_meeting_id}/
        artifacts-metadata.json
        annotated-{presentationName}.pdf
        shared-notes/notes.pdf
        sources/{presId}/...
    logs/
        recording-artifacts.log
        post_archive.log
        post_publish.log
```

`{prefix}` and the bucket are set by the BBB host (`BBB_RECORDING_ARTIFACTS_S3_PREFIX`
and `BBB_RECORDING_ARTIFACTS_S3_BUCKET`). The callback's `artifacts[].remote_file`
is a full `s3://bucket/key` URL — consumers can parse it instead of
reconstructing the path.

## Callback payload

```
POST {callback_url}
Content-Type: application/x-www-form-urlencoded

signed_parameters={JWT}
```

JWT is signed with `securitySalt` from `/etc/bigbluebutton/bbb-web.properties`
(same secret BBB uses for `bbb-recording-ready-url`). Decoded payload:

```json
{
  "meeting_id": "<parent_internal_id>",
  "artifacts": [
    {
      "meeting_id": "<internal_id>",
      "file": "/var/bigbluebutton/published/.../artifacts/annotated-deck.pdf",
      "remote_file": "s3://bucket/prefix/<parent>/annotated-deck.pdf",
      "type": "annotated-slides"
    },
    {
      "meeting_id": "<parent_internal_id>",
      "file": ".../shared-notes/notes.pdf",
      "remote_file": "s3://bucket/prefix/<parent>/shared-notes/notes.pdf",
      "type": "shared-notes",
      "format": "pdf"
    },
    {
      "meeting_id": "<parent_internal_id>",
      "file": ".../access-manifest.json",
      "remote_file": "s3://bucket/prefix/<parent>/access-manifest.json",
      "type": "access-manifest"
    },
    {
      "meeting_id": "<breakout_internal_id>",
      "file": ".../breakouts/<breakout>/annotated-deck.pdf",
      "remote_file": "s3://bucket/prefix/<parent>/breakouts/<breakout>/annotated-deck.pdf",
      "type": "annotated-slides"
    }
  ],
  "expected_artifacts": [
    "annotated-deck.pdf",
    "shared-notes/notes.pdf"
  ],
  "breakouts": [
    {
      "meeting_id": "<breakout_internal_id>",
      "expected_artifacts": ["annotated-deck.pdf", "shared-notes/notes.pdf"]
    }
  ]
}
```

### Field semantics

| Field | Meaning |
|---|---|
| `meeting_id` (top-level) | The **parent** meeting's internal id. The callback only fires for the parent — breakouts do not get their own callback. |
| `artifacts[]` | Flat list of every file uploaded across the parent and all breakouts. Order is not guaranteed. |
| `artifacts[].meeting_id` | The internal id of the meeting that owns this artifact. For breakout files this is the breakout's id, not the parent's. **This is authoritative for routing** — never assume `meeting_id` matches the top-level. |
| `artifacts[].file` | Local path on the BBB host. Useful for debugging; consumers should usually use `remote_file`. |
| `artifacts[].remote_file` | Full `s3://bucket/key` URL. Authoritative location. |
| `artifacts[].remote_files` | Present only for `raw-package` entries (a directory of files). Array of `s3://...` URLs. |
| `artifacts[].type` | See type table below. May be absent on entries produced by old script versions. |
| `artifacts[].format` | Only on `shared-notes`. One of `pdf`, `html`, `etherpad`, `txt`, `doc`, `odt` depending on what BBB archived. |
| `expected_artifacts` | Paths (relative to the parent's S3 prefix) that the script intended to upload for the parent. Compare against actual uploads to detect partial failures. |
| `breakouts[]` | One entry per breakout from the Phase 1 dump. **Every breakout appears here**, including those whose Phase 2 failed (their `expected_artifacts` will be empty). |
| `breakouts[].expected_artifacts` | Paths relative to the breakout's S3 prefix (`{prefix}/{parent}/breakouts/{breakout}/`). |

### `type` values

| Value | Description |
|---|---|
| `annotated-slides` | Per-presentation annotated PDF. |
| `shared-notes` | Etherpad shared notes export. Also carries `format`. |
| `artifacts-metadata` | The Phase 1 Postgres dump JSON (internal, but uploaded for rebuild capability). |
| `access-manifest` | The manifest described below. |
| `raw-package` | Fallback when Phase 1 had no dump — a directory of raw files for external processing. Carries `remote_files` (plural) instead of `remote_file`. |

### Routing rule for downstream

```python
artifact_by_meeting = defaultdict(list)
for art in payload["artifacts"]:
    artifact_by_meeting[art["meeting_id"]].append(art)
```

Never use `payload["meeting_id"]` to attach files. Use `art["meeting_id"]`.
This handles the case where the same external meeting has had multiple BBB
internal ids (e.g. re-creates) and a breakout's artifacts arrive in the
parent's callback.

## Access manifest (v3)

S3 location: `{prefix}/{parent_meeting_id}/access-manifest.json`. Mirrored
locally under the recording. Schema:

```json
{
  "version": 3,
  "meeting_id": "<parent_internal_id>",
  "ext_id": "<parent_external_id>",
  "expected_artifacts": ["annotated-deck.pdf", "shared-notes/notes.pdf"],
  "users": [
    {"ext_user_id": "...", "name": "...", "moderator": true}
  ],
  "breakouts": [
    {
      "meeting_id": "<breakout_internal_id>",
      "sequence": 1,
      "name": "Room 1",
      "expected_artifacts": ["annotated-deck.pdf"],
      "users": [{"ext_user_id": "...", "name": "...", "moderator": false}]
    }
  ]
}
```

`meeting_id` in the manifest is **always the parent's internal id**, even
in breakouts[].meeting_id which is the breakout's. `ext_id` is the external
id (the integration's id passed to the BBB `create` API).

### Manifest vs callback

These are designed to overlap, intentionally:

- `manifest.meeting_id` == `callback.meeting_id` (parent internal id)
- `manifest.expected_artifacts` == `callback.expected_artifacts`
- `manifest.breakouts[].meeting_id` == `callback.breakouts[].meeting_id`
- `manifest.breakouts[].expected_artifacts` == `callback.breakouts[].expected_artifacts`

The manifest adds the user lists and breakout name/sequence; the callback
adds the per-file `artifacts[]` listing with S3 URLs.

A consumer that processes the callback should also fetch the manifest to
get user/breakout identity info. The callback's
`artifacts.find(type == "access-manifest").remote_file` is the canonical
S3 URL for it.

### Migration from v2

Three breaking renames:

| v2 | v3 |
|---|---|
| `meetingId` (top-level) | `meeting_id` |
| `extId` (top-level) | `ext_id` |
| `breakouts[].meetingId` | `breakouts[].meeting_id` |

Everything else (`expected_artifacts`, `users[].ext_user_id`, etc.) is unchanged.

Consumers can detect the schema with `manifest["version"]`:

```python
v = manifest["version"]
if v >= 3:
    parent_mid = manifest["meeting_id"]
    parent_ext = manifest["ext_id"]
elif v == 2:
    parent_mid = manifest["meetingId"]
    parent_ext = manifest["extId"]
```

## Failure modes

### Partial / full failure

A `recording-artifacts.fail` file is uploaded to
`{prefix}/{parent}/recording-artifacts.fail` if any artifact failed. The
callback is **still sent** as long as at least one artifact uploaded
successfully. Inspect the fail file for the error list:

```json
{
  "meeting_id": "...",
  "mode": "prod",
  "timestamp": "2026-05-15T12:55:08+10:00",
  "errors": [
    {
      "meeting_id": "...",
      "scope": "parent",
      "type": "annotated-slides",
      "presId": "...",
      "error": "Timeout::Error: Timed out waiting for ... after 180s"
    }
  ]
}
```

If **every** artifact failed, no callback is sent and `recording-artifacts.fail`
will be the only marker.

### Phase 1 failure

If Phase 1 (Postgres snapshot at archive time) failed, the raw archive will
contain `artifacts-metadata.fail` instead of `artifacts-metadata.json`.
Phase 2 falls back to packaging raw files into `{prefix}/{parent}/raw-package/`
and a `raw-package` artifact appears in the callback. Annotated PDFs are
not generated in this case — the consumer must rebuild them externally
using the events.xml fallback tool included in this repo.

### Breakout-specific failures

Every breakout from the Phase 1 dump appears in `callback.breakouts[]` and
`manifest.breakouts[]`. Failures attached to specific breakouts surface as:

- `expected_artifacts: []` — breakout's Phase 2 failed, nothing uploaded for it.
- Entries in `recording-artifacts.fail`'s `errors[]` array scoped to
  `breakout:{breakout_internal_id}`.

## Per-meeting overrides

All server-wide settings can be overridden on the BBB `create` API call
using `meta_` parameters. The exporter looks up these case-insensitively.

| Server env var | `create` meta key | Notes |
|---|---|---|
| `BBB_RECORDING_ARTIFACTS_MODE` | `meta_artifactExportMode` | `dev` or `prod` |
| `BBB_RECORDING_ARTIFACTS_S3_BUCKET` | `meta_artifactExportS3Bucket` | Per-tenant buckets |
| `BBB_RECORDING_ARTIFACTS_S3_PREFIX` | `meta_artifactExportS3Prefix` | |
| `BBB_RECORDING_ARTIFACTS_S3_REGION` | `meta_artifactExportS3Region` | |
| `AWS_ACCESS_KEY_ID` | `meta_artifactExportAwsKeyId` | |
| `AWS_SECRET_ACCESS_KEY` | `meta_artifactExportAwsSecret` | |
| `BBB_RECORDING_ARTIFACTS_CALLBACK_URL` | `meta_artifactExportCallbackUrl` | This is how Django sets a per-tenant or per-environment callback URL |
| `BBB_RECORDING_ARTIFACTS_EXPORT_NOTES` | `meta_artifactExportNotes` | `true` / `false` |
| `BBB_RECORDING_ARTIFACTS_NOTES_FORMATS` | `meta_artifactExportNotesFormats` | Comma-separated |
| `BBB_RECORDING_ARTIFACTS_INCLUDE_BREAKOUTS` | `meta_artifactExportIncludeBreakouts` | `true` / `false` |
| `BBB_RECORDING_ARTIFACTS_LOCAL_FORMAT` | `meta_artifactExportLocalFormat` | Format directory under `published_dir` (`presentation` or `video`, depending on which BBB format is enabled) |

Meta overrides env. Both override per-mode defaults.

## Determinism and re-runs

- The callback fires **at most once per parent meeting** under normal flow.
  A `.done` marker at
  `/var/bigbluebutton/recording/status/published/{parent_meeting_id}-recording-artifacts.done`
  prevents re-runs across multiple format publishes (presentation + video).
- S3 PUTs are deduplicated on size: if an object already exists with
  matching `Content-Length`, the upload is skipped. Re-running after a
  partial failure resumes mid-way without re-uploading completed files.
- To force a re-run on the BBB host, delete the `.done` marker and re-invoke
  the hook manually.

## Versioning

| Surface | Version field | Where |
|---|---|---|
| Phase 1 dump (`artifacts-metadata.json`) | `version: 1` | Internal — consumers shouldn't usually read this |
| Access manifest | `version: 3` | Read this and branch on it |
| Callback payload | (no version field) | Field set is additive; consumers should ignore unknown keys |

If you need a callback `version` field, ask — adding one is cheap.
