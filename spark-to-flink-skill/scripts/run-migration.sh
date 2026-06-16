#!/usr/bin/env bash
# Run migration CLI from harness directory.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/harness"
uv run spark-flink-migrate "$@"
