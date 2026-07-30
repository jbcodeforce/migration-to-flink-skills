# Consistent repo-root `.env` loading Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every Python entry that needs LLM/Flink credentials loads `DOTENV_FILE` or `{monorepo_root}/.env`, matching `scripts/setup.sh`.

**Architecture:** Keep resolution in `flink_skill_common.config`; add public `find_repo_root()`. Fix callers that pass wrong `project_root`. Duplicate the same resolve rules in `cc-tools` (no dependency on flink-skill-common).

**Tech Stack:** Python 3.11+/3.12, `python-dotenv`, pytest

**Spec:** [docs/superpowers/specs/2026-07-29-dotenv-loading-design.md](../specs/2026-07-29-dotenv-loading-design.md)

## Global Constraints

- Override env var name is `DOTENV_FILE` only (not `DOT_ENV_FILE`)
- Default file is `{repo_root}/.env`
- Repo root marker: directory containing `references/flink/valid`
- `HarnessContext.project_root` = monorepo root; `harness_root` = skill package root
- Do not rename env vars; do not change shell `source` debug helpers
- Drop `CONFLUENT_ENV_FILE` / `~/.confluent/.env` from cc-tools

## File map

| File | Responsibility |
|------|----------------|
| `flink_skill_common/config.py` | `find_repo_root`, `_resolve_dotenv_path`, `configure`/`load_env` |
| `flink_sql_compare.py` | Import `find_repo_root` from config (if present) |
| `cli_validate.py`, `mcp/server.py` | Correct `HarnessContext` roots |
| `scripts/verify_setup.py` | Pass monorepo as `project_root` |
| `tests/it/conftest.py` (common), `debug.py` (ksql) | Correct roots |
| `cc_deploy/deploy_flink_statements.py` | Same dotenv rules via local helper |
| `test_config.py`, `cc-tools` UT | Cover resolve behavior |

---

### Task 1: `find_repo_root` + tests in config

**Files:**
- Modify: `flink-skill-common/harness/src/flink_skill_common/config.py`
- Modify: `flink-skill-common/harness/tests/ut/test_config.py`
- Modify (if tracked/used): `flink-skill-common/harness/src/flink_skill_common/flink_sql_compare.py` — import from config

**Interfaces:**
- Produces: `find_repo_root(start: Path | None = None) -> Path`

- [x] **Step 1:** Add failing tests `test_find_repo_root_*` in `test_config.py` (tmp tree with `references/flink/valid`; missing marker raises)
- [x] **Step 2:** Run tests — expect fail (symbol missing)
- [x] **Step 3:** Implement `find_repo_root` in `config.py`; re-export from `flink_sql_compare` if that module defines it
- [x] **Step 4:** Run `test_config.py` — pass
- [x] **Step 5:** Commit

### Task 2: Fix harness callers (`project_root` = monorepo)

**Files:**
- Modify: `cli_validate.py`, `mcp/server.py`, `scripts/verify_setup.py`
- Modify: `flink-skill-common/harness/tests/it/conftest.py` (if it still configures wrong root)
- Modify: `ksql-to-flink-skill/harness/tests/it/debug.py`
- Test: light asserts in `test_config.py` or existing smoke that configured context `project_root` name ends with monorepo / contains `references`

- [x] **Step 1:** Add failing test(s) that bootstrap paths resolve to monorepo `.env` location (or assert `get_context().project_root` after import patterns)
- [x] **Step 2:** Fix each caller: `harness_root` = skill package, `project_root` = monorepo (prefer `find_repo_root()` where parents are error-prone)
- [x] **Step 3:** Run unit tests + `verify_setup` path smoke if feasible
- [x] **Step 4:** Commit

### Task 3: Align `cc-tools` dotenv loading

**Files:**
- Modify: `cc-tools/src/cc_deploy/deploy_flink_statements.py`
- Create: `cc-tools/tests/ut/test_load_dotenv_file.py`
- Modify: docstring referencing `~/.confluent/.env`

- [x] **Step 1:** Failing UT: `DOTENV_FILE` wins; else repo-root `.env`; no `CONFLUENT_ENV_FILE`
- [x] **Step 2:** Implement local `find_repo_root` + `load_dotenv_file` per spec
- [x] **Step 3:** Run cc-tools UTs — pass
- [x] **Step 4:** Commit

### Task 4: Docs touch-up

**Files:**
- Modify only lines that document `CONFLUENT_ENV_FILE` / `~/.confluent/.env`

- [x] **Step 1:** Grep and update (docstring in `deploy_flink_statements.py`; no other user docs referenced `CONFLUENT_ENV_FILE`)
- [x] **Step 2:** Commit
