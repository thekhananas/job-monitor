#!/usr/bin/env bash
# Run from the Mac; SSH uses the existing key/config and known-host checks.
set -e
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
exec python3 -B "$SCRIPT_DIR/deploy.py" "$@"
