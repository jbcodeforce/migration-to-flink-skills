# Developer Guide

## Principles

* prepare KSQL or Spark sources with multiple statements in them, as separate DDLs or/and DMLs
* Still keep the source as part of the context.
* Assign different rules for different scope

## Tools and dev practices
### flink-skill-common

The role of this component is to process the generated Flink SQL with static analysis or deployment using Confluent Cloud for Flink REST API. 

The component includes LLM factory, and one dedicated agent to validate the SQL and try to fix issues.

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
