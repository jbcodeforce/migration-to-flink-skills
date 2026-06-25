"""Agno agent for fixing Flink SQL validation and deploy failures."""

from __future__ import annotations

from pathlib import Path

from flink_skill_common.agents.factory import (
    build_migration_agent,
    make_openai_model,
    run_agent_response,
)
from flink_skill_common.config import (
    agent_fixer_max_retries,
    flink_skill_common_skill_dir,
    llm_api_key,
    llm_base_url,
)
from flink_skill_common.deploy.llm_tools import FlinkStatementLLMTools
from flink_skill_common.deploy.statements import (
    ddl_statement_name,
    discover_source_ddl_files,
    dml_statement_name,
)
from flink_skill_common.llm import resolve_llm_model


def _make_model():
    return make_openai_model(
        base_url=llm_base_url(),
        api_key=llm_api_key(),
        model_id=resolve_llm_model(),
    )


def build_deploy_fixer_agent():
    """Agent with confluent-sql tools for fixing failed Flink SQL validation or deploys."""
    deploy_tools = FlinkStatementLLMTools()
    return build_migration_agent(
        name="FlinkSqlDeployFixerAgent",
        skill_dir=flink_skill_common_skill_dir(),
        instructions=[
            "Fix Flink SQL validation and deploy failures for migration harnesses.",
            "Call get_skill_instructions('validate-flink-sql') before fixing SQL.",
            "Read skill reference confluent-sql-deploy.md for deploy tool sequence.",
            "Deploy source stub DDLs from tests/ddl.*.sql before target DDL and DML.",
            "Use create_flink_statement to redeploy source DDLs, then target DDL, then DML.",
            "Use get_flink_statement_exceptions to understand failures.",
            "Use list_flink_statements or get_flink_statement to verify RUNNING phase.",
            "Statement names: {table}-ddl and {table}-dml with underscores replaced by hyphens.",
            "If DML fails because a source table is missing, regenerate stub DDL in tests/.",
            "Return corrected source DDLs, DDL, and DML in labeled ```sql blocks.",
        ],
        model=_make_model(),
        tools=deploy_tools.as_tools(),
    )


def deploy_fixer_prompt(
    table_name: str,
    source_sql: str,
    source_label: str,
    ddl_path: Path,
    dml_path: Path,
    error_message: str,
    tests_dir: Path | None = None,
) -> str:
    ddl = ddl_path.read_text() if ddl_path.is_file() else ""
    dml = dml_path.read_text() if dml_path.is_file() else ""
    ddl_stmt = ddl_statement_name(table_name)
    dml_stmt = dml_statement_name(table_name)

    source_section = ""
    if tests_dir and tests_dir.is_dir():
        source_files = discover_source_ddl_files(tests_dir)
        if source_files:
            blocks = []
            for _table, path in source_files:
                blocks.append(f"{path.name}:\n```sql\n{path.read_text().strip()}\n```")
            source_section = (
                "Source stub DDLs (tests/ — deploy these before target DDL/DML):\n"
                + "\n\n".join(blocks)
                + "\n\n"
            )

    return (
        f"Flink SQL fix required for table `{table_name}`.\n\n"
        f"Error:\n{error_message}\n\n"
        f"Target statement names: DDL `{ddl_stmt}`, DML `{dml_stmt}`.\n\n"
        f"Original {source_label}:\n```sql\n{source_sql.strip()}\n```\n\n"
        f"{source_section}"
        f"Current DDL ({ddl_path}):\n```sql\n{ddl.strip()}\n```\n\n"
        f"Current DML ({dml_path}):\n```sql\n{dml.strip()}\n```\n\n"
        "1. Call get_flink_statement_exceptions for the failed statement when deployed.\n"
        "2. Fix source stub DDLs in tests/ if missing-table errors; then target DDL/DML.\n"
        "3. Redeploy via create_flink_statement (source DDLs, target DDL, then DML).\n"
        "4. Confirm RUNNING with get_flink_statement or list_flink_statements.\n"
        "Return corrected source DDLs, DDL, and DML in labeled ```sql blocks."
    )


def run_agent_deploy_fixer(
    table_name: str,
    source_sql: str,
    source_label: str,
    ddl_path: Path,
    dml_path: Path,
    error_message: str,
    tests_dir: Path | None = None,
) -> str:
    """Invoke Agno agent with confluent-sql tools to fix and redeploy failed statements."""
    agent = build_deploy_fixer_agent()
    prompt = deploy_fixer_prompt(
        table_name,
        source_sql,
        source_label,
        ddl_path,
        dml_path,
        error_message,
        tests_dir=tests_dir,
    )
    max_retries = agent_fixer_max_retries()
    last_content = ""
    for attempt in range(max_retries):
        last_content = run_agent_response(
            agent,
            f"{prompt}\n\nRetry attempt {attempt + 1} of {max_retries}.",
        )
        if "RUNNING" in last_content.upper() and "FAILED" not in last_content.upper():
            break
    return last_content
