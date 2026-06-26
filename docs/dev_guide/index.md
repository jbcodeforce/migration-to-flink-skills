# Developer Guide



## Processing Flow for Flink SQL validation

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
