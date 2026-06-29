# ksql CLI `migrate` — design & flow

## Sequence diagram

End-to-end flow for `ksql-flink-migrate migrate --table <name> --file <path> [--out-dir output] [--skip-deploy]`.

```mermaid
sequenceDiagram
    autonumber
    actor User
    participant CLI as cli.migrate
    participant Progress as ProgressReporter
    participant Config as flink_skill_common.config
    participant LLMCheck as flink_skill_common.llm
    participant KsqlUtils as ksql_utils
    participant Migrate as migrate_agent
    participant Factory as agents.factory
    participant KsqlAgent as KsqlToFlinkAgent<br/>(Agno + ksql-to-flink skill)
    participant LLM as LLM server
    participant Pipeline as pipeline<br/>clean_flink_sql_and_validate
    participant Output as flink_skill_common.output
    participant Sources as sources
    participant SourceAgent as SourceDdlAgent
    participant Convergence as convergence<br/>converge_flink_sql
    participant SqlValidate as sql_validate
    participant DeployFixer as deploy_fixer
    participant FixerAgent as FlinkSqlDeployFixerAgent<br/>(Agno + validate-flink-sql skill)
    participant FSM as FlinkStatementManager
    participant CC as Confluent Cloud Flink<br/>(confluent-sql REST)

    User->>CLI: ksql-flink-migrate migrate -t table -f file.ksql [-o out_dir] [--skip-deploy]
    CLI->>Config: configure(HarnessContext)
    CLI->>Progress: banner(table, file, out_dir, model, agent_fixer, log)
    Progress-->>User: echo config banner

    CLI->>CLI: file.exists()
    alt file not found
        CLI-->>User: typer.Exit(1)
    end

    CLI->>Progress: step(1, "Checking LLM...")
    CLI->>LLMCheck: llm_reachable()
    LLMCheck->>LLM: GET /models
    alt LLM unreachable
        LLMCheck-->>CLI: false
        CLI-->>User: typer.Exit(1)
    end
    LLMCheck-->>CLI: true
    CLI->>Progress: done(1, "LLM reachable")

    CLI->>KsqlUtils: split_ksql_create_statements(file.read_text())
    KsqlUtils-->>CLI: ksql_statements[]
    alt no CREATE STREAM/TABLE found
        CLI-->>User: typer.Exit(1)
    end

    CLI->>CLI: _discover_statement_names(ksql_statements)
    Note over CLI,KsqlUtils: per statement: clean_ksql_input() + extract_ksql_object_name()
    CLI->>Progress: done(2, "Found N CREATE statement(s)")

    loop for each ksql CREATE statement
        CLI->>KsqlUtils: clean_ksql_input(ksql_statement)
        KsqlUtils-->>CLI: ksql_cleaned
        CLI->>KsqlUtils: extract_ksql_object_name(ksql_cleaned)
        KsqlUtils-->>CLI: source_name
        CLI->>Progress: header("[i/N] source_name → table")

        rect rgb(240, 248, 255)
            Note over CLI,LLM: Phase 1 — LLM translation
            CLI->>Progress: step(2, "Running translation agent...")
            CLI->>Migrate: run_migration(table, ksql_cleaned, source_name, on_event)
            Migrate->>Migrate: build_ksql_migrate_agent()
            Migrate->>Factory: build_migration_agent(name, skill_dir, instructions, model)
            Factory-->>Migrate: KsqlToFlinkAgent
            Migrate->>Migrate: migrate_prompt(table, ksql, source_name)
            Migrate->>Factory: run_agent_response(agent, prompt, on_event)
            Factory->>KsqlAgent: agent.run(prompt, stream=True)
            KsqlAgent->>KsqlAgent: get_skill_instructions("ksql-to-flink")
            KsqlAgent->>LLM: chat completion (translate ksql → Flink DDL + DML)
            LLM-->>KsqlAgent: ```sql DDL ... ``` ```sql DML ... ```
            KsqlAgent-->>Factory: response content
            Factory-->>Migrate: response string
            Migrate-->>CLI: response
            CLI->>Progress: done(2, "Translation agent finished")
        end

        rect rgb(255, 250, 240)
            Note over CLI,CC: Phase 2 — extract, write, validate, deploy
            CLI->>Progress: step(3, "Extracting SQL blocks and writing output...")
            CLI->>Pipeline: clean_flink_sql_and_validate(response, table, ksql_cleaned, skip_deploy, out_dir)

            Pipeline->>Output: extract_sql_blocks(response)
            Output-->>Pipeline: ddls[], dmls[]
            Pipeline->>Output: write_output(table, ddls, dmls, out_dir)
            Output-->>Pipeline: ddl_paths[], dml_paths[]

            alt DML present
                Pipeline->>KsqlUtils: compute_missing_source_tables(dml, table, ddl)
                KsqlUtils-->>Pipeline: missing[]
                opt missing source tables
                    Pipeline->>Sources: generate_source_ddls(table, src_ksql, dml, missing)
                    Sources->>SourceAgent: agent.run(source_ddl_prompt)
                    SourceAgent->>LLM: generate stub CREATE TABLE DDL (JSON)
                    LLM-->>SourceAgent: source DDL stubs
                    SourceAgent-->>Sources: parsed source_ddls
                    Sources-->>Pipeline: source_ddls dict
                    Pipeline->>Output: write_source_ddls(out_dir, source_ddls)
                    Output-->>Pipeline: tests/ddl.*.sql paths
                end
            end

            Pipeline->>Convergence: converge_flink_sql(ddls, dmls, ConvergenceContext, skip_deploy)

            loop convergence attempt (1..agent_fixer_max_retries)
                Convergence->>SqlValidate: validate_syntax_for_statements(ddls, dmls)
                Note over SqlValidate: sqlglot parse (read="flink")
                SqlValidate-->>Convergence: offline_issues[]

                Convergence->>Output: write_output + resolve_table_paths
                Output-->>Convergence: ddl_path, dml_path

                alt offline validation errors
                    opt agent_fixer enabled
                        Convergence->>Convergence: _apply_agent_fix(ctx, ddl_path, dml_path, errors)
                        Convergence->>DeployFixer: run_agent_deploy_fixer(...)
                        DeployFixer->>Factory: build_deploy_fixer_agent()
                        Factory-->>DeployFixer: FixerAgent (+ FlinkStatementLLMTools)
                        DeployFixer->>FixerAgent: run_agent_response(prompt + error)
                        FixerAgent->>CC: create_flink_statement / get_flink_statement_exceptions (via tools)
                        FixerAgent->>LLM: fix SQL from validation errors
                        LLM-->>FixerAgent: corrected ```sql blocks
                        FixerAgent-->>DeployFixer: response
                        DeployFixer-->>Convergence: response
                        Convergence->>Output: extract_sql_blocks + write_source_ddls
                        Note over Convergence: continue loop with updated ddls/dmls
                    else agent_fixer disabled
                        Convergence-->>Pipeline: raise SqlValidationError
                    end
                else offline validation passed
                    alt skip_deploy
                        Convergence-->>Pipeline: ConvergenceResult(success=True)
                    else deploy enabled
                        alt no tests/ directory
                            Convergence-->>Pipeline: ConvergenceResult(success=False)
                        else
                            Convergence->>FSM: deploy_table(table, ddl_path, dml_path, tests_dir)
                            FSM->>FSM: _deploy_source_ddls(tests/ddl.*.sql)
                            loop each source stub DDL
                                FSM->>CC: create_statement(source-ddl-name, sql)
                                FSM->>CC: _wait_for_deploy_phase()
                            end
                            FSM->>CC: create_statement({table}-ddl, ddl_sql)
                            FSM->>CC: _wait_for_deploy_phase()
                            opt DML present
                                FSM->>CC: create_statement({table}-dml, dml_sql)
                                FSM->>CC: _wait_for_deploy_phase()
                                FSM->>CC: check_statement_health(dml_name)
                            end
                            CC-->>FSM: DeployResult(ddl_phase, dml_phase, success)
                            FSM-->>Convergence: DeployResult

                            alt deploy success
                                Convergence-->>Pipeline: ConvergenceResult(success=True)
                            else deploy failed / unhealthy
                                opt agent_fixer enabled
                                    Convergence->>Convergence: _apply_agent_fix(ctx, error_message)
                                    Note over Convergence,FixerAgent: same agent fix loop as offline errors
                                else agent_fixer disabled
                                    Convergence-->>Pipeline: ConvergenceResult(success=False)
                                end
                            end
                        end
                    end
                end
            end

            Convergence-->>Pipeline: ConvergenceResult
            alt convergence failed
                Pipeline-->>CLI: raise typer.Exit(1)
            end
            Pipeline-->>CLI: (ddl_path, dml_path) or None if skip_deploy
            CLI->>Progress: done(3..5, validation/deploy status)
        end
    end

    CLI-->>User: "Done. Processed N statement(s). Output: out_dir"
```

## Participants summary

| Component | Module | Role |
|-----------|--------|------|
| `cli.migrate` | `ksql_to_flink/cli.py` | Typer entry point; orchestrates per-statement loop |
| `ProgressReporter` | `ksql_to_flink/cli_progress.py` | Terminal step / agent event output |
| `ksql_utils` | `ksql_to_flink/ksql_utils.py` | Split, clean, and name ksql CREATE statements |
| `run_migration` | `ksql_to_flink/migrate_agent.py` | Builds KsqlToFlinkAgent and runs LLM translation |
| `build_migration_agent` / `run_agent_response` | `flink_skill_common/agents/factory.py` | Agno agent construction and streaming run |
| `clean_flink_sql_and_validate` | `ksql_to_flink/pipeline.py` | Extract SQL, write files, source stubs, convergence |
| `generate_source_ddls` | `ksql_to_flink/sources.py` | LLM-generated stub DDL for missing DML sources |
| `converge_flink_sql` | `flink_skill_common/convergence.py` | Offline validate → deploy → agent-fix retry loop |
| `validate_syntax_for_statements` | `flink_skill_common/sql_validate.py` | sqlglot Flink dialect syntax check |
| `run_agent_deploy_fixer` | `flink_skill_common/agents/deploy_fixer.py` | Agent with confluent-sql tools to fix failures |
| `FlinkStatementManager.deploy_table` | `flink_skill_common/deploy/flink_statement_manager.py` | Deploy source DDLs, target DDL, then DML to CC |

## Output layout

After a successful run, `out_dir` typically contains:

```
out_dir/
├── ddl.{table}.sql          # target table DDL
├── dml.{table}.sql          # INSERT INTO … SELECT … (if CSAS)
└── tests/
    └── ddl.{source}.sql     # stub DDL for upstream tables referenced in DML
```
