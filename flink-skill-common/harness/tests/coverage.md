# Test coverage: `flink_skill_common`

Maps `harness/src/flink_skill_common/` functions to unit tests (`tests/ut/`) and integration tests (`tests/it/`). Indirect coverage is noted where relevant.

- **182 unit tests** across 14 files (`uv run pytest tests/ut --collect-only`)
- **11 integration tests** across 3 files (`uv run pytest tests/it --collect-only`)

Integration tests use `references/flink/` fixtures and require Confluent Cloud credentials (`require_deploy`) and a reachable LLM (`require_llm`) where marked. For that be sure to set environment variables in the `.env` under the repository folder.

## Unit tests summary

| Module | Test file | Tests | Tested | Untested |
|--------|-----------|-------|--------|----------|
| `config.py` | `test_config.py`, `test_deploy_fixer.py` | 41 (+1 indirect) | 29 symbols | `_configure_cli_logging`, `llm_reachable`, `fetch_models_payload` -> done in IT|
| `sql_parse.py` | `test_sql_parse.py`, `test_with_property_rules.py` | 14 (+4 indirect) | 16 functions | — |
| `with_property_rules.py` | `test_with_property_rules.py` | 6 | 1 | — |
| `response_io.py` | `test_response_io.py` | 21 | 11 (+ helpers indirect) | `_write_sql_files` (indirect) |
| `sql_validate.py` | `test_sql_validate.py` | 28 | 10 | — |
| `convergence.py` | `test_convergence.py` | 20 | 7 (+ integration) | `_emit_progress`, `_logger` |
| `skill_adapt.py` | `test_adapt_skills.py`, `test_skill_adapt.py` | 6 | 2 | — |
| `agents/skill_loaders.py` | `test_skill_adapt.py`, `test_sources.py`, `test_deploy_fixer.py` | (shared) | 1 class | — |
| `agents/factory.py` | `test_factory.py` | 3 | 1 (+ `_tool_name` indirect) | 5 |
| `agents/table_source_agent.py` | `test_sources.py` | 9 | 4 | `_make_model` |
| `agents/cc_deployment_fixer.py` | `test_deploy_fixer.py` | 3 | 1 | 3 |
| `cli_progress.py` | — | 0 | — | 1 class |
| `cli_validate.py` | — | 0 | — | 7 |
| `deploy/flink_statement_manager.py` | `test_flink_statement_manager.py` | 7 | 14 functions | deploy, check_health, validate_* (indirect) |
| `deploy/llm_tools.py` | — | 0 | — | 1 class |
| `mcp/server.py` | `test_mcp_server.py` | 11 | 7 tools (+ registration) | — |
| `sqlglot_flink/` | `test_flink_dialect.py` | 7 | dialect + expressions | `FlinkParser` (indirect) |

---

## `config.py` — `test_config.py` (41 tests)

| Function / class | Unit test(s) |
|------------------|--------------|
| `resolve_dotenv_path` | `test_resolve_dotenv_*` (4 tests) |
| `HarnessContext` | `test_config`, `test_skill_dir` |
| `FlinkDeploySettings` | `test_flink_deploy_settings_*` |
| `FlinkDeployNotReadyError` | `test_flink_deploy_settings_missing_raises` |
| `configure` | all tests (fixture setup) |
| `get_context` | `test_config`, `test_get_context_raises_when_not_configured` |
| `load_env` | `test_load_env_*` (2 tests) |
| `dotenv_path` | `test_dotenv_path_accessor` |
| `llm_base_url`, `llm_model`, `llm_api_key`, `llm_timeout` | `test_llm_defaults`, `test_llm_env_overrides` |
| `flink_deploy_poll_seconds`, `flink_deploy_timeout_seconds` | `test_flink_deploy_timing_*` (2 tests) |
| `flink_org_id` | `test_flink_org_id_aliases` |
| `flink_env_id` | `test_flink_env_id_aliases` |
| `flink_compute_pool_id` | `test_flink_compute_pool_id_aliases` |
| `flink_catalog_name` | `test_flink_catalog_name_aliases` |
| `flink_database_name` | `test_flink_database_name` |
| `flink_api_key`, `flink_api_secret` | `test_flink_api_credentials_aliases` |
| `flink_rest_endpoint` | `test_flink_rest_endpoint_*` (2 tests) |
| `skill_dir`, `skill_md_path` | `test_skill_dir` |
| `flink_skill_common_skill_dir` | `test_skill_dir`, `test_deploy_fixer.py::test_flink_skill_common_skill_dir` |
| `validate_flink_sql_skill_dir` | `test_skill_dir`, `test_deploy_fixer.py::test_flink_skill_common_skill_dir` |
| `agent_fixer_enabled` | `test_agent_fixer_enabled_*` (2 tests) |
| `agent_fixer_max_retries` | `test_agent_fixer_max_retries_override` |
| `flink_deploy_settings` | `test_flink_deploy_settings_*`, `test_agent_settings_from_shared_dotenv`; `test_scenario.py::test_validate_config` |
| `get_logger` | `test_logger` |
| `_configure_cli_logging` | — |
| `cli_log_file` | `test_cli_log_file_*` (3 tests) |
| `cli_log_level` | `test_cli_log_level_default_and_override` |
| `llm_reachable` | `it/conftest.py::require_llm` (IT skip fixture only) |
| `fetch_models_payload` | — |

---

## `sql_parse.py` — `test_sql_parse.py` (14 tests)

| Function | Unit test(s) |
|----------|--------------|
| `strip_sql_comments_and_drops` | `test_strip_sql_comments_and_drops`, `test_strip_set_statements` |
| `split_create_statements` | `test_split_create_statements` |
| `split_ddl_statements`, `split_dml_statements` | `test_split_ddl_and_dml_statements` |
| `extract_ddl_table_name`, `extract_dml_table_name` | `test_extract_ddl_and_dml_table_names` |
| `extract_statement_table_name` | `test_extract_ddl_and_dml_table_names`; `test_flink_statement_manager.py::test_extract_table_name` |
| `is_create_table_statement`, `is_insert_into_statement` | `test_statement_kind_helpers` |
| `extract_cte_names` | `test_extract_cte_names` |
| `extract_created_table_names` | `test_extract_created_table_names` |
| `extract_dml_source_tables` | `test_extract_dml_source_tables`, `test_extract_dml_source_tables_excludes_ctes_and_target`, `test_extract_dml_source_tables_empty` |
| `compute_missing_source_tables` | `test_compute_missing_source_tables`, `test_compute_missing_source_tables_none_when_defined_in_ddl` |
| `extract_ddl_with_block`, `parse_with_properties` | `test_with_property_rules.py` (4 tests) |
| `extract_sql_blocks` (integration) | `test_extract_sql_blocks` (via `response_io`) |

---

## `with_property_rules.py` — `test_with_property_rules.py` (6 tests)

| Function | Unit test(s) |
|----------|--------------|
| `validate_with_properties` | `test_validate_with_properties_*` (6 tests) |

---

## `response_io.py` — `test_response_io.py` (21 tests)

| Function | Unit test(s) |
|----------|--------------|
| `strip_markdown_fence` | `test_strip_markdown_fence` |
| `_normalize_sql` | `test_normalize_sql` |
| `_split_statements` | `test_split_statements` |
| `extract_sql_blocks` | `test_extract_labeled_sql_blocks`, `test_extract_labeled_sql_blocks_without_columns`, `test_extract_json_migration`, `test_extract_sequential_sql_blocks`, `test_extract_sql_blocks_empty`, `test_extract_sql_blocks_normalizes_json_newlines`, `test_extract_sql_blocks_splits_multiple_statements`; `test_sql_parse.py::test_extract_sql_blocks` |
| `_extract_labeled_sql_blocks` | indirect via `extract_sql_blocks` |
| `_extract_json_migration` | indirect via `test_extract_json_migration` |
| `_extract_sequential_sql_blocks` | indirect via `test_extract_sequential_sql_blocks` |
| `parse_source_ddls_from_response` | `test_parse_source_ddls_from_response`, `test_parse_source_ddls_invalid_json`, `test_parse_source_ddls_normalizes_escaped_newlines` |
| `write_output` | `test_write_output_one_file_per_statement`, `test_write_output_duplicate_table_suffix`, `test_write_output_uses_fallback_name` |
| `resolve_table_paths` | `test_resolve_table_paths`, `test_resolve_table_paths_disambiguated_suffix` |
| `write_source_ddls` | `test_write_source_ddls_layout` |
| `_disambiguated_stem` | `test_disambiguated_stem` |
| `_write_sql_files` | indirect via `write_output` / `write_source_ddls` |

Table names for `write_output` use `sql_parse.extract_ddl_table_name` / `extract_dml_table_name` (also covered in `test_sql_parse.py`).

---

## `sql_validate.py` — `test_sql_validate.py` (28 tests)

| Function / class | Unit test(s) |
|------------------|--------------|
| `SqlValidationIssue`, `SqlValidationError` | `test_sql_validation_error_message`, `test_raise_on_errors_*`; `test_convergence.py` |
| `_extract_ddl_header` | `test_extract_ddl_header` |
| `_parse_error_message` | `test_parse_error_message` |
| `_validate_parseable` | `test_validate_watermark_parseable`, `test_validate_virtual_metadata_parseable`, `test_validate_dml_parseable` |
| `_validate_one` | `test_validate_one_*` (5 tests), `test_validate_one_invalid_value_format` |
| `validate_syntax_for_statements` | `test_validate_statements_*` (5 tests), `test_validate_all_flink_references`, `test_validate_ctas`, `test_offline_*` (7 fixture tests) |
| `log_validation_issues` | `test_log_validation_issues` |
| `raise_on_errors` | `test_raise_on_errors_raises`, `test_raise_on_errors_ignores_warnings` |
| `validate_statements_remote` | `test_validate_statements_remote` (mocked); `test_validation_it.py` (remote E2E) |

Offline fixture cases (formerly in `test_validation_it.py`):

| Test | Fixture case |
|------|--------------|
| `test_offline_valid_raw_classical_songs` | `valid/raw_classical_songs` |
| `test_offline_valid_watermark_metadata` | `valid/watermark_metadata` |
| `test_offline_rejects_bad_syntax` | `invalid/ddl_bad_syntax` |
| `test_offline_fixable_properties` | `invalid/ddl_fixable_typo` |
| `test_offline_rejects_bad_dml` | `invalid/dml_bad_syntax` |
| `test_offline_multiple_errors` | `invalid/multi_error_convergence` |

---

## `convergence.py` — `test_convergence.py` (20 tests)

| Function / class | Unit test(s) |
|------------------|--------------|
| `ConvergenceContext`, `ConvergenceResult` | fixtures and assertions throughout |
| `converge_flink_sql` | `test_converge_*` (10 tests); `test_convergence_it.py::test_converge_valid_deploy` (deploy E2E) |
| `clean_flink_sql_and_validate` | `test_clean_flink_sql_and_validate_*` (3 tests); `test_scenario.py::test_add_source_tables_with_agent` (E2E) |
| `_format_validation_errors` | `test_format_validation_errors_includes_line` |
| `_deploy_messages` | `test_deploy_messages_success`, `test_deploy_messages_unhealthy` |
| `_apply_agent_fix` | `test_apply_agent_fix_*` (3 tests) |
| `_resolve_paths` | indirect via `converge_flink_sql` / `_apply_agent_fix` |
| `_emit_progress`, `_logger` | — |

---

## `skill_adapt.py` — `test_adapt_skills.py` (4 tests), `test_skill_adapt.py` (2 tests)

| Function | Unit test(s) |
|----------|--------------|
| `parse_skill_name` | `test_parse_skill_name_from_frontmatter` |
| `adapt_skill_content` | `test_adapt_skill_content_*` (2 tests), `test_validate_flink_sql_cursor_excludes_agno_fixer`, `test_agno_adapted_skill_strips_cursor_write_instructions` |

---

## `agents/skill_loaders.py` — shared across agent tests

| Class | Unit test(s) |
|-------|--------------|
| `AgnoAdaptedLocalSkills` | `test_agno_adapted_local_skills_loader`, `test_local_skills_loads_validate_flink_sql`, `test_local_skills_loads_source_ddl`, `test_multi_skill_root_loads_both_skills` |

---

## `agents/factory.py` — `test_factory.py` (3 tests)

| Function | Unit test(s) |
|----------|--------------|
| `run_agent_response` | `test_run_agent_response_*` (3 tests) |
| `_tool_name` | indirect via `test_run_agent_response_maps_stream_events_to_callback` |
| `make_openai_model` | — |
| `resolve_llm_model` | mocked in `test_deploy_fixer.py::test_build_deploy_fixer_agent` |
| `fetch_available_models` | `test_scenario.py::test_list_models` (IT) |
| `fetch_model_context_windows` | — |
| `build_skilled_agent` | mocked in `test_sources.py::test_generate_source_ddls_*` |
| `_normalize_model_name` | — |

---

## `agents/table_source_agent.py` — `test_sources.py` (9 tests)

| Function | Unit test(s) |
|----------|--------------|
| `_source_ddl_skill_dir` | `test_source_ddl_skill_dir` |
| `_source_ddl_prompt` | `test_source_ddl_prompt_includes_inputs` |
| `generate_source_ddls` | `test_generate_source_ddls_*` (4 tests) |
| `_make_model` | — |
| `AgnoAdaptedLocalSkills` (skill loading) | `test_local_skills_loads_source_ddl`, `test_build_source_ddl_agent_uses_source_ddl_skill`, `test_multi_skill_root_loads_both_skills` |

---

## `agents/cc_deployment_fixer.py` — `test_deploy_fixer.py` (3 tests)

| Function | Unit test(s) |
|----------|--------------|
| `build_deploy_fixer_agent` | `test_build_deploy_fixer_agent` |
| `deploy_fixer_prompt` | — |
| `run_agent_deploy_fixer` | mocked in `test_convergence.py` (not end-to-end) |
| `_make_model` | — |

---

## `cli_progress.py`

| Class | Unit test(s) | Integration test(s) |
|-------|--------------|---------------------|
| `ProgressReporter` | — | `test_scenario.py::test_add_source_tables_with_agent` |

---

## `cli_validate.py`

| Function | Unit test(s) | Integration test(s) |
|----------|--------------|---------------------|
| `read_sql_files` | — | `flink_ref_fixtures.load_flink_pair` (used by UT fixture tests and IT) |
| `remote` | — | `test_scenario.py::test_validate_good_flink_sql`, `test_validate_bad_flink_sql`, `test_fix_bad_sql_with_agent` |
| `syntax_only`, `validate_flink_sqls`, `main` | — | — |
| `_issue_dict`, `_validation_result`, `_emit_result`, `_read_sql_file_from_paths`, `_classify_fixture` | — | — |

---

## `deploy/flink_statement_manager.py` — `test_flink_statement_manager.py` (7 tests)

| Function / class | Unit test(s) |
|------------------|--------------|
| `classify_sql` | `test_classify_sql` |
| `FlinkStatementManager.create_statement` | `test_create_statement_snapshot_ddl`, `test_create_statement_retries_on_409` |
| `FlinkStatementManager.wait_for_phase` | `test_wait_for_phase_success`, `test_wait_for_phase_timeout` |
| `FlinkStatementManager.get_statement_exceptions` | `test_get_statement_exceptions` |
| `StatementManagerError`, `DeployError`, `DeployResult` | `test_flink_statement_manager.py`, `test_convergence.py` |
| `_statement_phase`, `_statement_detail` | indirect via manager methods |
| `normalize_statement_prefix`, `ddl_statement_name`, `dml_statement_name`, `discover_source_ddl_files` | — |
| `FlinkStatementManager.deploy`, `check_health`, `validate_statements`, `connect` | indirect via `converge_flink_sql` mocks; deploy E2E in `test_convergence_it.py`, `test_scenario.py` |
| `FlinkStatementManager.list_statements` | `test_scenario.py::test_list_statements` |
| `FlinkStatementManager.drop_table`, `delete_statement` | `cleanup_deployed_table`; IT teardown for `remote()` in `test_scenario.py` |

---

## `deploy/llm_tools.py`

| Class | Unit test(s) |
|-------|--------------|
| `FlinkStatementLLMTools` | — (used by MCP server and deploy fixer agent; no direct unit test) |

---

## `mcp/server.py` — `test_mcp_server.py` (11 tests)

| Function | Unit test(s) |
|----------|--------------|
| `validate_flink_sql_offline` | `test_validate_flink_sql_offline_*` (2 tests) |
| `validate_flink_sql_remote` | `test_validate_flink_sql_remote_*` (2 tests) |
| `create_flink_statement`, `wait_flink_statement_phase`, `get_flink_statement_exceptions`, `check_flink_statement_health` | `test_mcp_server.py` (1 test each) |
| tool registration | `test_mcp_registers_expected_tool_names` |
| `_get_deploy_tools`, `main` | `test_get_deploy_tools_caches_instance`, `test_main_runs_mcp_server` |

---

## `sqlglot_flink/` — `test_flink_dialect.py` (7 tests)

| Symbol | Unit test(s) |
|--------|--------------|
| `Flink` dialect | `test_flink_dialect_registered` |
| `Watermark`, `MetadataColumnConstraint` | parse tests |
| `FlinkParser` | indirect via dialect parse tests |
| Parse: watermark, metadata, combined DDL | `test_parse_*` (5 tests) |
| Malformed input | `test_malformed_watermark_raises` |

---

## Integration tests summary

| Test file | Tests | Fixtures | Primary modules |
|-----------|-------|----------|-----------------|
| `it/test_validation_it.py` | 2 | `require_deploy` | `validate_statements_remote` |
| `it/test_convergence_it.py` | 2 | `require_deploy` | `converge_flink_sql`, `FlinkStatementManager.deploy`, `drop_table` |
| `it/test_scenario.py` | 7 | `require_deploy`, `require_llm` (2 tests) | `flink_deploy_settings`, `FlinkStatementManager`, `cli_validate.remote`, `clean_flink_sql_and_validate`, `fetch_available_models`, `ProgressReporter` |

### IT fixtures — `tests/it/conftest.py`

| Fixture | Purpose |
|---------|---------|
| `_configure_harness_context` | `configure(HarnessContext)` for every IT test |
| `_clear_logs_file` | Clears `cli_log_file()` before each test |
| `require_deploy` | Skips when `flink_deploy_settings()` raises `FlinkDeployNotReadyError` |
| `require_llm` | Skips when `llm_reachable()` is false |

### Shared helpers — `tests/flink_ref_fixtures.py`

| Function | Used by |
|----------|---------|
| `load_flink_pair` | IT tests; `test_sql_validate.py` offline fixture tests |
| `validation_issues` | `test_validation_it.py`; `test_sql_validate.py` offline fixture tests |
| `assert_no_errors`, `assert_has_errors` | `test_validation_it.py`; `test_sql_validate.py` offline fixture tests |
| `load_all_valid_flink_reference_sql` | `test_sql_validate.py::test_validate_all_flink_references` |
| `assert_convergence_stages` | — (unused; candidate for removal) |

---

## `it/test_validation_it.py` (2 tests)

Remote validation against `references/flink/` fixture cases. Offline cases moved to `test_sql_validate.py`.

| Test | Fixture case | Function(s) | `require_deploy` |
|------|--------------|-------------|------------------|
| `test_remote_valid_raw_classical_songs` | `valid/raw_classical_songs` | `validate_statements_remote` | yes |
| `test_remote_rejects_missing_pk` | `invalid/ddl_missing_pk` | `validate_statements_remote` | yes |

---

## `it/test_convergence_it.py` (2 tests)

End-to-end convergence and deploy against `references/flink/valid/routing/filtering/`.

| Test | Function(s) | Notes |
|------|-------------|-------|
| `test_load_pair` | `flink_ref_fixtures.load_flink_pair` | Asserts 2 DDL + 1 DML loaded |
| `test_converge_valid_deploy` | `converge_flink_sql`, `FlinkStatementManager.cleanup_deployed_table` | Full deploy loop; CC cleanup via convergence finally |

---

## `it/test_scenario.py` (7 tests)

Deploy connectivity, remote validation, LLM listing, and source-table agent E2E.

| Test | Function(s) | Fixture / notes | Fixtures |
|------|-------------|-----------------|----------|
| `test_validate_config` | `flink_deploy_settings` | — | `require_deploy` |
| `test_list_statements` | `FlinkStatementManager.list_statements` | — | `require_deploy` |
| `test_list_models` | `fetch_available_models`, `llm_base_url` | Asserts `Ornith-1.0-9B-6bit` served | `require_llm` |
| `test_validate_good_flink_sql` | `cli_validate.remote` | `valid/routing/filtering/*` | `require_deploy` |
| `test_validate_bad_flink_sql` | `cli_validate.remote` | `invalid/multi_error_convergence/*` | `require_deploy` |
| `test_fix_bad_sql_with_agent` | `cli_validate.remote` | `invalid/multi_error_convergence/*` (remote only; name is legacy) | `require_deploy` |
| `test_add_source_tables_with_agent` | `clean_flink_sql_and_validate`, `ProgressReporter` | Synthetic LLM response + `tests/tmp/table_test` layout | `require_deploy`, `require_llm` |

Run integration tests:

```bash
cd flink-skill-common/harness
uv run pytest tests/it -m integration
```
