# Design: Consistent repo-root `.env` loading

**Date:** 2026-07-29  
**Status:** Approved for planning  
**Approach:** Centralize resolve + fix all callers (Approach 1)

## Problem

Python entry points that need LLM/Flink credentials must load the same env file as `scripts/setup.sh`: `DOTENV_FILE` if set, otherwise `{monorepo_root}/.env`.

Today `flink_skill_common.config` already implements that resolution, but several callers pass a wrong `project_root` (skill package instead of monorepo), so they miss the shared `.env`. `cc-tools` uses a separate convention (`CONFLUENT_ENV_FILE` / `~/.confluent/.env`).

## Decisions

| Topic | Choice |
|-------|--------|
| Override env var | Keep **`DOTENV_FILE`** (not `DOT_ENV_FILE`) |
| Scope | Audit all Python that needs env vars |
| `cc-tools` | Bring onto the same `DOTENV_FILE` / repo-root `.env` convention |

## Section 1: Resolution rules

**Canonical override:** `DOTENV_FILE` only (absolute path, or relative to monorepo root).

**Default:** `{repo_root}/.env` when `DOTENV_FILE` is unset.

**Repo root:** walk parents until `references/flink/valid` exists. Move/reuse `find_repo_root` from `flink_sql_compare.py` into `config.py` so dotenv and path helpers share one finder. `flink_sql_compare` re-exports or imports from `config`.

**Load behavior (unchanged):** `load_dotenv(path, override=True)`; missing file → no-op / `False`, not an error.

**`HarnessContext` contract:**

- `project_root` = monorepo root (where `.env` lives)
- `harness_root` = skill package root (`flink-skill-common`, `ksql-to-flink-skill`, `spark-to-flink-skill`, …)

## Section 2: Caller audit & fixes

### Fix (wrong / incomplete today)

| Site | Issue |
|------|--------|
| `scripts/verify_setup.py` | `project_root = harness_root.parent` → skill package, not monorepo; subprocesses miss repo `.env` |
| `flink_skill_common/cli_validate.py` | `project_root` = `flink-skill-common`, not monorepo |
| `flink_skill_common/mcp/server.py` | `harness_root` / `project_root` parents off by one |
| `flink-skill-common/.../it/conftest.py` | `project_root=HARNESS_ROOT.parent` instead of `REPO_ROOT` |
| `ksql-to-flink/.../it/debug.py` | same off-by-one |
| `cc-tools/.../deploy_flink_statements.py` | `CONFLUENT_ENV_FILE` / `~/.confluent/.env` |

### Leave alone (already correct)

- `ksql_to_flink/config.py`
- `spark_flink_skill/config.py`
- `flink_qa_agent.py`
- Most UT `configure()` calls with `__PROJECT_ROOT = __COMMON_ROOT.parent`

### `cc-tools`

Replace `load_dotenv_file()` to resolve via `DOTENV_FILE` else `{find_repo_root()}/.env`. Drop `CONFLUENT_ENV_FILE` / home-dir default. Update its IT accordingly.

If `cc-tools` cannot depend on `flink_skill_common`, duplicate a minimal `find_repo_root` + dotenv resolve (same rules) rather than introduce a new package.

### Out of scope

- Shell scripts that `source .env` for debug attach
- Broad doc rewrites; only update lines that already document the old `cc-tools` / `~/.confluent/.env` path

## Section 3: Tests & rollout

### Unit tests

- Extend `test_config.py`: `find_repo_root` finds monorepo (or tmp fixture with `references/flink/valid`)
- Keep existing `_resolve_dotenv_path` / `DOTENV_FILE` / default `.env` tests; ensure relative paths resolve against monorepo `project_root`
- Optional light smoke that `cli_validate` / MCP bootstrap use monorepo as `project_root`

### `cc-tools`

- Update IT that calls `load_dotenv_file()`
- Add UT: prefers `DOTENV_FILE`, then repo-root `.env`; no `CONFLUENT_ENV_FILE`

### `verify_setup.py`

- Call `configure(..., project_root=repo_root)` (monorepo) so LLM/agent checks load the shared `.env`

### Docs

- One-line swaps where `cc-tools` / `~/.confluent/.env` is documented

### Success criteria

Any Python entry that needs LLM/Flink credentials loads the same file as `scripts/setup.sh` (`DOTENV_FILE` or `$REPO_ROOT/.env`).

## Non-goals

- Renaming `DOTENV_FILE` → `DOT_ENV_FILE`
- Forcing every test file to load `.env` when it does not need credentials
- Changing shell `source` debug helpers beyond noting they remain separate
