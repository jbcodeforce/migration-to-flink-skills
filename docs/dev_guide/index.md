# Developer Guide

## Principles

* prepare KSQL or Spark sources with multiple statements in them, as separate DDLs or/and DMLs
* Still keep the source as part of the context.
* Assign different rules for different scope

## Components

![](../images/arch.drawio.png)

### flink-skill-common

The role of this component is to process the generated Flink SQL with static analysis or deployment using Confluent Cloud for Flink REST API and to offer a set of common tools, used for migration.

| Important Features | Code | Principles |
| ------ |-------|-----------|
| Centralize configuration, logs, load ,env | config.py | Common skill and specific skill loading |
| Factory to build agno agents | agents.factory.py | |
| Agent for fixing Flink SQL syntax validation and deployment failures.| cc_deployment_fixer.py| Use syntaxic parser, confluent cloud statement deployment and LLM to fix the error and save results to target folder |


* Unit tests without backends
    ```sh
    cd flink-skill-common/harness
    uv run pytest -vs tests/ut
    ```
* For integration tests, be sure the LLM server is reachable.
    ```sh
    uv run pytest -vs tests/it/
    ```
## References

The references folder includes flink migrated statements from ksql and [Confluent tutorial]()

## Application Flows

### Processing Flow for Flink SQL validation

```mermaid
sequenceDiagram
    participant Loop as converge_flink_sql
    participant Offline as sqlglot
    participant Agent as LLM_agent
    participant Remote as CC_Flink
    participant Deploy as deploy_table

    Loop->>Offline: attempt 1
    Offline-->>Loop: DML error INSRT
    Loop->>Agent: fix offline errors
    Agent-->>Loop: corrected DML
    Loop->>Offline: attempt 2
    Offline-->>Loop: pass
    Loop->>Remote: validate_statements_remote
    Remote-->>Loop: DDL invalid format
    Loop->>Agent: fix remote errors
    Agent-->>Loop: corrected DDL
    Loop->>Offline: attempt 3
    Offline-->>Loop: pass
    Loop->>Remote: pass
    Loop->>Deploy: deploy_table
    Deploy-->>Loop: success
```
