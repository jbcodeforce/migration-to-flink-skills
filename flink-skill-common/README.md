# flink-skill-common

Shared Python library for the ksql-to-flink and spark-to-flink migration skill harnesses.

## Modules

| Module | Purpose |
|--------|---------|
| `compare` | Unordered line comparison for golden SQL tests |
| `output` | Parse LLM migration responses; write DDL/DML files |
| `llm` | OpenAI-compatible LLM reachability and model resolution |
| `config` | `HarnessContext`, LLM settings, `FlinkDeploySettings`, deploy preflight |
| `sql_preprocess` | Comment/DROP stripping and CREATE statement splitting |
| `fixtures` | `GoldenPair` dataclass and fixture existence checks |
| `agents.factory` | Agno agent construction helpers |
| `deploy` | Confluent Cloud Flink deploy via confluent-sql REST driver |

## Usage

Each skill harness calls `configure()` once at import time in its thin `config.py`, then depends on this package via an editable path dependency:

```toml
dependencies = ["flink-skill-common"]

[tool.uv.sources]
flink-skill-common = { path = "../../flink-skill-common", editable = true }
```

## Commands

```bash
uv sync --extra dev
uv run pytest
```
