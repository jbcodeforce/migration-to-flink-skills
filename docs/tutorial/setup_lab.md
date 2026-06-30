# Lab: Setup

Prepare the local environment for Agno-based migration CLIs (ksqlDB and Spark SQL to Flink SQL).

## Prerequisites

- macOS or Linux (lab tested on Mac)
- [uv](https://docs.astral.sh/uv/getting-started/installation/) on `PATH`
- Python 3.11+ (`uv python install 3.11` if needed)
- A running OpenAI-compatible LLM server (for example oMLX) at the URL configured in `.env`

## Run setup

From the repository root:

```bash
./scripts/setup.sh
```

The script will:

1. Check `uv` and Python 3.11+
2. Create `.env` from `.env.example` if missing
3. Run `uv sync --extra dev` in `flink-skill-common`, `ksql-to-flink-skill`, and `spark-to-flink-skill` harnesses
4. Verify Python packages, Agno agent construction, all CLI entry points, and LLM reachability
5. Generate Cursor skills under `.cursor/skills/` and Claude skills under `*/.claude/skills/` via `adapt_skills.py`

Re-run verification without reinstalling:

```bash
./scripts/setup.sh --skip-sync
```

## LLM configuration

Edit the repo-root `.env` (or set `DOTENV_FILE` to an external file):

| Variable | Purpose |
|----------|---------|
| `SL_LLM_BASE_URL` | OpenAI-compatible API base (default is local `http://localhost:7999/v1`) |
| `SL_LLM_MODEL` | Model id from response of  `curl GET $SL_LLM_BASE_URL/models` |
| `SL_LLM_API_KEY` | API key if required by your server |

Setup **fails** if the LLM server is not reachable or the configured model is missing or has a context window below 8000 tokens. Start your local inference server before running setup.

## Optional: Flink deploy credentials

Translate-only runs use `--skip-deploy` and do not need Confluent Cloud credentials. For deploy, fill `FLINK_*` variables in `.env`.

```bash
cp .env.example .env
export DOTENV_FILE=/path/to/reusable.env  # optional -- default is repository  .env
```

Fill LLM and Flink credentials in `.env`

## Verified CLIs

| CLI | Harness directory |
|-----|-------------------|
| `flink-skill-mcp`, `flink-skill-validate` | `flink-skill-common/harness` |
| `ksql-flink-migrate`, `ksql-flink-agent` | `ksql-to-flink-skill/harness` |
| `spark-flink-migrate`, `spark-flink-agent` | `spark-to-flink-skill/harness` |

*Developer: see the `piproject.toml` under flink-skill-common, ksql-to-flink-skill and spark-to-flink-skill*

## Skills: Agno vs Cursor

| Runtime | Skill source | Validation |
|---------|--------------|------------|
| Agno harness / CLI | `skill/SKILL.md` (canonical) | `flink-skill-validate` CLI or `skill/scripts/validate_offline.py` |
| Cursor / Claude IDE | `.cursor/skills/` (generated) | MCP `validate_flink_sql_offline` on `flink-skill-common` server |

As a developer or for tuning the skill, edit the canonical `skill/SKILL.md`, then refresh the IDE skills:

```bash
./scripts/adapt-skills.sh --target cursor
./scripts/adapt-skills.sh --target claude
```

