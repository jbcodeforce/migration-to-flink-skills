# Introduction

This repository provides AI-assisted tooling to migrate SQL workloads to **Confluent Flink SQL**. It covers translation from **Spark SQL** and **ksqlDB**, followed by offline validation and optional deployment to Confluent Cloud for Flink.

The project is organized as agent skills, Python harnesses, and reference examples. Translation, validation, and deploy-fix loops are split into dedicated agents so each step can run locally or inside an IDE.

## Audience

This documentation is for:

- **Data engineers and platform teams** planning a move from Spark SQL or ksqlDB to Confluent Flink
- **Developers** who want to run or extend the migration CLIs and validation workflow on their own machine
- **Practitioners exploring agentic AI** for SQL migration using skills in Cursor, Claude Code, or the Agno harness

No prior experience with the Agno framework is required to follow the tutorials. Familiarity with Flink SQL, ksqlDB, or Spark SQL is helpful.

## What you will learn

After working through this site, you will be able to:

1. **Set up the lab environment** — install dependencies, configure a local or remote LLM, and generate IDE skill variants
2. **Migrate source SQL to Flink SQL** — translate ksqlDB or Spark SQL using the migration agents and reference examples
3. **Validate and refine Flink SQL** — run offline checks, remote validation against Confluent Cloud, and an automated fix loop for common errors
4. **Choose a runtime** — use the Agno CLI harness, Cursor with MCP, or Claude Code skills depending on your workflow
5. **Understand the architecture** — how translation, validation, and convergence agents fit together, and where to extend the tooling

Start with the [Setup Lab](tutorial/setup_lab.md), then follow the [Migration AI Lab](tutorial/migration_ai_lab.md). Contributors can use the [Developer Guide](dev_guide/index.md) for implementation details.
