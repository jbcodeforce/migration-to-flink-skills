# Migration to Confluent Flink Skill and Tools

This repository is a consolidation of multiple initiatives I have started since 18 months on using AI to migrate from Spark SQL to Flink SQL and from Confluent KsqlDB to Flink SQL. The initial implementation was using OpenAI API to interact with local LLM running with Ollama.In 2026,  I am adopting Agno as a framework to do agentic solution. Claude Code and other generic agentic cli leverage skills to do a lot of automation. 

I am convince that dedicated agents will be more efficient than generic LLM, and also local LLM inference is needed to avoid sending SQL code to Frontier LLM that keep communication information. Also long term memory, managed locally will enhance the quality of the migration over time: the more it is used, the better it should be.

I'm also convince that small LLM, well prompted, with efficients tool can do a lot of such migrations. I plan also to do fine tuning for small LLM on multiple use case.

## Repository structure

* [flink-skill-common](./flink-skill-common/) includes python code for common tools
* [ksql-to-flink-skil](./ksql-to-flink-skill/) includes skills, tests and tools for KSQLDB to Flink SQL
* [spark-to-flink-skill](./spark-to-flink-skill/.) includes skills, tests and tools for Spark SQL to Flink SQL
* [references](./references/) includes example of sparks and ksql code to migrate to Flink SQL with matching Flink SQL.

## Documentation

## Environment setup

All harnesses share one environment file at the repo root:

```bash
cp .env.example .env
# edit .env with LLM and Flink credentials
```

To reuse an external env file across projects:

```bash
export DOTENV_FILE=/path/to/my-reusable.env
```

See [flink-skill-common/README.md](./flink-skill-common/README.md) for details.

