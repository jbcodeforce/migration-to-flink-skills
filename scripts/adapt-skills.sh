#!/usr/bin/env bash
# Adapt canonical Agno skills for Cursor, Claude, or Agno runtimes.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
exec python3 "$REPO_ROOT/scripts/adapt_skills.py" "$@"
