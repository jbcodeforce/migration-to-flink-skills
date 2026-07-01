# Lab: Migration using AI

The current AI based migration implementation supported by this tool enables migration of:

* Spark SQL to Flink SQL
* ksqlDB to Flink SQL

The approach uses LLM agents local or remote. After this lab you should be able to use the `migration-to-flink` tools to partially automate your SQL migration to Flink SQL.

The core idea is to leverage LLMs and parser tools to understand the source SQL semantics and to translate them to Flink SQLs. 

**This is github repository is not production ready, the LLM can generate hallucinations, and one to one mapping between source like ksqlDB or Spark to Flink is sometime not the best approach.** We expect that this agentic solution could be a strong foundation for better results, and can be enhanced over time.

**Migration** is a one time shot, and should not be a practice to develop Flink solution.

???+ warning "Lab Environment"
	The Lab was developed and tested on Mac.

## Prerequisites

Be sure to have done the [Setup Lab](setup_lab.md) and [Setup script](../../scripts/setup.sh) to get different CLIs operational and generate Cursor/Claude skill variants from the canonical Agno `skill/` directories.

## Different runtimes

**Agno harness (CLI)** — translation and validation run via Python agents. The harness loads `skill/SKILL.md` directly. Flinl SQL validation and corection uses `flink-skill-validate` or skill scripts under `flink-skill-common/skill/scripts/`.

**Cursor (IDE)** — skills under `.cursor/skills/` are generated with MCP-oriented instructions. Validation uses the `flink-skill-common` MCP server (`validate_flink_sql_offline`, `validate_flink_sql_remote`). Enable MCP in Cursor Settings.

**Claude Code (IDE)** — skills under `*/.claude/skills/` plus the same shell CLIs as the Agno harness (`flink-skill-validate`, `ksql-flink-migrate`). No MCP setup is required for this lab.

After editing any canonical `skill/SKILL.md`, run `./scripts/adapt-skills.sh --target cursor` and/or `./scripts/adapt-skills.sh --target claude` before using IDE workflows.

### Claude Code integration

This section covers two skill scopes: Flink SQL validation and ksqlDB to Flink migration. Skills provide agent playbooks; CLIs run deterministic checks and translation.

| Scope | Skill | Package | Generated Claude path |
|-------|-------|---------|------------------------|
| Flink SQL validation | `/validate-flink-sql` | [`flink-skill-common`](../../flink-skill-common/) | `flink-skill-common/.claude/skills/validate-flink-sql/` |
| ksqlDB to Flink migration | `/ksql-to-flink` | [`ksql-to-flink-skill`](../../ksql-to-flink-skill/) | `ksql-to-flink-skill/.claude/skills/ksql-to-flink/` |
| spark SQL to Flink migration |  |  |


## Setup

Assume [Setup Lab](setup_lab.md) has already run (`./scripts/setup.sh`). That generates Claude skill copies under each package's `.claude/skills/`.


1. **Environment** — repo-root `.env` with `SL_LLM_*` variables. Offline validation does not need an LLM; migration CLI does.
1. **Optional remote validation / deploy** — fill `FLINK_*` in `.env` only if you run `flink-skill-validate remote` or `ksql-flink-migrate` without `--skip-deploy`.


## Scope 1: Flink SQL validation

### Happy path

Use when you already have Flink DDL and/or DML and need syntax checks or convention fixes, and deployment using Confluent Cloud.

#### CLI (from repo root):

```bash
cd flink-skill-common/harness && uv sync --extra dev

uv run flink-skill-validate offline \
  --ddl ../../references/flink/valid/raw_classical_songs/ddl.raw_classical_songs.sql \
  --dml ../../references/flink/valid/raw_classical_songs/dml.raw_classical_songs.sql
```

The CLI prints JSON with `"ok": true/false` and an `issues` array. Exit code 0 means pass; 1 means validation errors.

#### Claude Code

Start `claude` code under the `references/flink` folder.

> Load the `validate-flink-sql` skill. Run `flink-skill-validate offline`. on the Flink SQL in valid/filtering/ . If validation fails, apply the skill rules, write corrected SQL, and re-run until `ok` is true.

You should get a response like:
```sh
Validation passed on the first run — no fixes needed.

Result:
{
  "ok": true,
  "issues": [],
  "error_count": 0
}

I loaded the validate-flink-sql skill and ran flink-skill-validate offline on the two Flink SQL files in valid/filtering/:

- ddl.filtered_pub.sql — CREATE TABLE filtered_publications (has PRIMARY KEY ... NOT ENFORCED, DISTRIBUTED BY HASH(book_id), and standardized connector properties)
- dml.filtered_pub.sql — INSERT INTO filtered_publications ... SELECT ... WHERE author = 'George R. R. Martin'

Both are syntactically valid per the offline (sqlglot Flink-dialect) check: ok is true with zero issues. No corrections were required, so no re-run was needed. These files live under valid/, so passing is the expected outcome.
```


### Lab exercise — invalid fixture

Validate broken DML from [`references/flink/invalid/dml_bad_syntax/`](../../references/flink/invalid/dml_bad_syntax/) (contains an `INSRT INTO` typo):

```bash
cd flink-skill-common/harness
uv run flink-skill-validate offline \
  --dml ../../references/flink/invalid/dml_bad_syntax/dml.sql
```

**Expected:** exit code 1 and a json like:
```json
{
  "ok": false,
  "issues": [
    {
      "statement_index": 0,
      "kind": "dml",
      "message": "DML must start with INSERT INTO",
      "line": null,
      "severity": "error"
    }
  ],
  "error_count": 1
}
```

**Follow-up:** Ask Claude to fix the DML using `validate-flink-sql` rules and re-run validation until it passes.
```sh
using /validate-flink-sq assess invalid/dml_bad_syntax/dml.sql
```

you should get something like:
```sh
Validation failed — as expected for this invalid/ fixture.

File: invalid/dml_bad_syntax/dml.sql
INSRT INTO raw_classical_songs(artist, title) VALUES ('Test Artist', 'Test Title');

Result:
{
  "ok": false,
  "issues": [
    {
      "statement_index": 0,
      "kind": "dml",
      "message": "DML must start with INSERT INTO",
      "line": null,
      "severity": "error"
    }
  ],
  "error_count": 1
}

Assessment: The statement's keyword is misspelled — INSRT instead of INSERT — so the validator rejects it as not starting with INSERT INTO. The corrected form would be:

INSERT INTO raw_classical_songs (artist, title) VALUES ('Test Artist', 'Test Title');

Since this lives under invalid/ (a negative test fixture meant to fail), I've left it unchanged and only reported the finding. Let me know if you'd like me to write the corrected version somewhere.
```

???+ tip "Alternative invalid fixture"
	For validating a DDL syntax error instead, use [`references/flink/invalid/ddl_bad_syntax/ddl.sql`](../../references/flink/invalid/ddl_bad_syntax/ddl.sql) with `--ddl` only. Expect `"kind": "ddl"` in the issues.

### Scope 2: ksqlDB to Flink migration with CC validation

Use when converting ksqlDB `CREATE STREAM` / `CREATE TABLE` scripts to Flink DDL and DML.

**CLI** (translate-only, from repo root):

```bash
cd ksql-to-flink-skill/harness && uv sync --extra dev

uv run ksql-flink-migrate \
  --table dim_all_songs \
  --file ../../references/ksql/sources/routing/merge.ksql \
  --out-dir ../../staging/ksql-lab-out 
```

Validate generated output:

```bash
uv run --directory flink-skill-common/harness flink-skill-validate offline \
  --ddl ../../staging/ksql-lab-out/ddl.dim_all_songs.sql \
  --dml ../../staging/ksql-lab-out/dml.dim_all_songs.sql
```

**Example Claude prompt:**

> Load the `ksql-to-flink` skill. Migrate `references/ksql/sources/routing/merge.ksql` for table `dim_all_songs` using `ksql-flink-migrate`. Then validate output with `flink-skill-validate offline`.

**Workflow:**

1. Apply `ksql-to-flink` translation rules (or run `ksql-flink-migrate` CLI).
2. Write `ddl.{table}.sql` and `dml.{table}.sql` under `--out-dir`.
3. Run `flink-skill-validate offline` on the outputs.
4. On errors, apply `validate-flink-sql` rules and re-validate.

#### Lab exercise 2 — merge.ksql

| Input | Target table | Golden reference (optional) |
|-------|--------------|----------------------------|
| [`references/ksql/sources/routing/merge.ksql`](../../references/ksql/sources/routing/merge.ksql) | `dim_all_songs` | [`references/flink/valid/dimensions/songs/all_song/`](../../references/flink/valid/dimensions/songs/all_song/) |

Run the migration and validation commands above. Compare output structure against the golden reference if you want a manual sanity check.

### Optional: remote validation and deploy

These steps require `FLINK_*` credentials in repo-root `.env`.

**Remote validation** (Confluent Cloud Flink parser):

```bash
uv run --directory flink-skill-common/harness flink-skill-validate remote \
  --ddl path/to/ddl.sql \
  --dml path/to/dml.sql
```

**Full migration with deploy** (omit `--skip-deploy`):

```bash
uv run --directory ksql-to-flink-skill/harness ksql-flink-migrate \
  --table dim_all_songs \
  --file ../../references/ksql/sources/routing/merge.ksql \
  --out-dir /tmp/ksql-lab-out
```

See [flink-skill-common README](../../flink-skill-common/README.md) and [ksql-to-flink-skill FLINK_DEPLOY.md](../../ksql-to-flink-skill/docs/FLINK_DEPLOY.md) for credential details.
