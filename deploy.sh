#!/usr/bin/env bash
set -euo pipefail

# Deploy BBB recording artifact post-hooks into the BBB recording pipeline
# and restart the RAP services.

if [[ $EUID -ne 0 ]]; then
    echo "This script must be run as root (use sudo)." >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BBB_SCRIPTS_DIR="/usr/local/bigbluebutton/core/scripts"

SRC_POST_ARCHIVE="${SCRIPT_DIR}/post_archive/post_archive_recording_artifacts.rb"
SRC_POST_PUBLISH="${SCRIPT_DIR}/post_publish/post_publish_recording_artifacts.rb"
SRC_ENV_EXAMPLE="${SCRIPT_DIR}/.env.example"

DST_POST_ARCHIVE_DIR="${BBB_SCRIPTS_DIR}/post_archive"
DST_POST_PUBLISH_DIR="${BBB_SCRIPTS_DIR}/post_publish"
DST_ENV="/etc/default/bbb-recording-artifacts"

for f in "$SRC_POST_ARCHIVE" "$SRC_POST_PUBLISH" "$SRC_ENV_EXAMPLE"; do
    if [[ ! -f "$f" ]]; then
        echo "Missing source file: $f" >&2
        exit 1
    fi
done

for d in "$DST_POST_ARCHIVE_DIR" "$DST_POST_PUBLISH_DIR"; do
    if [[ ! -d "$d" ]]; then
        echo "Missing destination directory: $d" >&2
        exit 1
    fi
done

echo "Installing post_archive hook..."
cp "$SRC_POST_ARCHIVE" "$DST_POST_ARCHIVE_DIR/"

echo "Installing post_publish hook..."
cp "$SRC_POST_PUBLISH" "$DST_POST_PUBLISH_DIR/"

if [[ -e "$DST_ENV" ]]; then
    echo "Skipping ${DST_ENV} (already exists; edit in place to update)."
else
    echo "Installing ${DST_ENV} from .env.example..."
    cp "$SRC_ENV_EXAMPLE" "$DST_ENV"
    echo "  -> edit ${DST_ENV} to set S3 bucket and AWS credentials."
fi

echo "Restarting bbb-rap-starter..."
systemctl restart bbb-rap-starter

echo "Restarting bbb-rap-resque-worker..."
systemctl restart bbb-rap-resque-worker

echo "Deployment complete."
