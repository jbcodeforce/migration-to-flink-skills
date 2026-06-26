#!/usr/bin/env bash
# Run from a shell, then start "fsc Pytest (attach only)" in Cursor.
# Or use "fsc Pytest (start + attach)" to run this script automatically.
set -euo pipefail

HARNESS_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$HARNESS_ROOT"

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

TEST_TARGET="${FSC_DEBUG_TEST_TARGET:-tests/it/test_convergence_it.py}"
PORT="${FSC_DEBUGPY_PORT:-5678}"

echo "Waiting for debugger attach on 127.0.0.1:${PORT} ..."
echo "pytest target: ${TEST_TARGET}"
exec "${HARNESS_ROOT}/.venv/bin/python" -Xfrozen_modules=off -m debugpy --listen "127.0.0.1:${PORT}" --wait-for-client \
  -m pytest "${TEST_TARGET}" -v -s
