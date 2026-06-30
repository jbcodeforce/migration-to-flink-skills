# Lab: Migration using AI

The current AI based migration implementation supported by this tool enables migration of:

* Spark SQL to Flink SQL
* ksqlDB to Flink SQL

The approach uses LLM agents local or remote. After this lab you should be able to use the `migration-to-flink` tool to partially automate your SQL migration to Flink SQL.

The core idea is to leverage LLMs and parser tools to understand the source SQL semantics and to translate them to Flink SQLs. 

**This is github repositiory is not production ready, the LLM can generate hallucinations, and one to one mapping between source like ksqlDB or Spark to Flink is sometime not the best approach.** We expect that this agentic solution could be a strong foundation for better results, and can be enhanced over time.

**Migration** is a one time shot, and should not be a practice to develop Flink solution.

???+ warning "Lab Environment"
	The Lab was developed and tested on Mac.

## Prerequisites

Be sure to have done the [Setup script](../../scripts/setup.sh) to get different CLI operational and generate Cursor/Claude skill variants from the canonical Agno `skill/` directories.

## Two runtimes

**Agno harness (CLI)** — translation and validation run via Python agents. The harness loads `skill/SKILL.md` directly. Validation uses `flink-skill-validate` or skill scripts under `flink-skill-common/skill/scripts/`.

**Cursor / Claude (IDE)** — skills under `.cursor/skills/` are generated with MCP-oriented instructions. Validation uses the `flink-skill-common` MCP server (`validate_flink_sql_offline`, `validate_flink_sql_remote`). Enable MCP in Cursor Settings.

After editing any canonical `skill/SKILL.md`, run `./scripts/adapt-skills.sh --target cursor` (and `--target claude` if needed) before using IDE workflows.

