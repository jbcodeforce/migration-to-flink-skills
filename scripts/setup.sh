#!/usr/bin/env bash
# Install and verify all migration CLI harnesses (Agno + local LLM).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SKIP_SYNC=0

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Install uv dependencies for all migration harnesses and verify local Agno setup.

Options:
  --skip-sync   Skip uv sync (verify only; harnesses must already be installed)
  --help        Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-sync) SKIP_SYNC=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
  esac
done

step() {
  echo ""
  echo "==> $1"
}

die() {
  echo "ERROR: $1" >&2
  exit 1
}

step "Checking prerequisites"
command -v uv >/dev/null 2>&1 || die "uv is not installed. See https://docs.astral.sh/uv/getting-started/installation/"
if ! uv python find 3.11 >/dev/null 2>&1; then
  die "Python 3.11+ required. Run: uv python install 3.11"
fi
if ! command -v curl >/dev/null 2>&1; then
  echo "WARNING: curl not found (optional; used for LLM troubleshooting in README)"
fi
echo "  uv and Python 3.11+ ok"

step "Ensuring environment file"
ENV_FILE="${DOTENV_FILE:-$REPO_ROOT/.env}"
if [[ -n "${DOTENV_FILE:-}" && ! "$ENV_FILE" =~ ^/ ]]; then
  ENV_FILE="$REPO_ROOT/$ENV_FILE"
fi
if [[ ! -f "$ENV_FILE" ]]; then
  if [[ -f "$REPO_ROOT/.env.example" ]]; then
    cp "$REPO_ROOT/.env.example" "$REPO_ROOT/.env"
    echo "  Created $REPO_ROOT/.env from .env.example — edit SL_LLM_* and optional Flink credentials"
  else
    die "Missing .env and .env.example at repo root"
  fi
else
  echo "  Using env file: $ENV_FILE"
fi

sync_harness() {
  local dir="$1"
  local name="$2"
  step "Installing $name"
  (cd "$dir" && uv sync --extra dev)
}

if [[ "$SKIP_SYNC" -eq 0 ]]; then
  sync_harness "$REPO_ROOT/flink-skill-common/harness" "flink-skill-common"
  sync_harness "$REPO_ROOT/ksql-to-flink-skill/harness" "ksql-to-flink-skill"
  sync_harness "$REPO_ROOT/spark-to-flink-skill/harness" "spark-to-flink-skill"
else
  step "Skipping uv sync (--skip-sync)"
fi

step "Verifying dependencies, Agno agents, CLIs, and LLM"
(
  cd "$REPO_ROOT/flink-skill-common/harness"
  unset VIRTUAL_ENV
  PYTHONUNBUFFERED=1 uv run python "$REPO_ROOT/scripts/verify_setup.py" --repo-root "$REPO_ROOT"
)

step "Adapting skills for Cursor and Claude"
python3 "$REPO_ROOT/scripts/adapt_skills.py" --target cursor --repo-root "$REPO_ROOT"
python3 "$REPO_ROOT/scripts/adapt_skills.py" --target claude --repo-root "$REPO_ROOT"

cat <<EOF

Setup complete. Migration CLIs ready:

  flink-skill-mcp          (flink-skill-common/harness)
  flink-skill-validate     (flink-skill-common/harness)
  ksql-flink-migrate       (ksql-to-flink-skill/harness)
  spark-flink-migrate      (spark-to-flink-skill/harness)

Examples:

  cd ksql-to-flink-skill/harness
  uv run ksql-flink-migrate --table my_table --file path/to.ksql --out-dir output/ --skip-deploy

  cd spark-to-flink-skill/harness
  uv run spark-flink-migrate --table my_table --file path/to.sql --out-dir output/ --skip-deploy

Deploy to Confluent Cloud requires FLINK_* credentials in .env (optional for translate-only runs).

Cursor/Claude skills generated under .cursor/skills/ and */.claude/skills/ (run scripts/adapt-skills.sh to refresh).
EOF
