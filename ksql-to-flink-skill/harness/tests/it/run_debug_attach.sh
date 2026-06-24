#!/usr/bin/env bash
# Run from a shell that can reach the LLM (e.g. Terminal.app with VPN).
# Then start "k2f debug.py (attach)" in the VS Code/Cursor debugger.
set -euo pipefail

HARNESS_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$HARNESS_ROOT"

export PYTHONPATH="${HARNESS_ROOT}/src:${HARNESS_ROOT}/../../flink-skill-common/src"

if [[ -n "${DOTENV_FILE:-}" && -f "${DOTENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${DOTENV_FILE}"
  set +a
elif [[ -f "${HARNESS_ROOT}/../../.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${HARNESS_ROOT}/../../.env"
  set +a
fi

echo "Waiting for debugger attach on 127.0.0.1:5678 ..."
exec "${HARNESS_ROOT}/.venv/bin/python" -m debugpy --listen 127.0.0.1:5678 --wait-for-client tests/it/debug.py
