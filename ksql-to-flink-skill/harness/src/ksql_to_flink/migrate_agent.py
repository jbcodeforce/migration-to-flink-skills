"""
Copyright 2024-2026 Confluent, Inc.
KSQL to Flink SQL Translation Agent

This module provides functionality to translate KSQL (Kafka SQL) statements to Apache Flink SQL
using Large Language Model (LLM) agents. The translation process includes multiple validation
steps and can handle both single and multiple table/stream definitions.

This module implements a multi-step workflow for translating KSQL to Flink SQL:
    1. Input cleaning (remove DROP statements and comments)
    2. Table detection (identify multiple CREATE statements)
    3. Translation using LLM agents
    4. Mandatory validation and syntax checking
    5. Optional semantic validation against live Flink environment
    6. Iterative refinement based on error feedback

Use Agno agent with skills to translate KSQL to Flink SQL.
"""

from __future__ import annotations

import sys
from pathlib import Path

from flink_skill_common.agents.factory import (
    build_migration_agent,
    make_openai_model,
    run_agent_response,
)
from flink_skill_common.deploy.llm_tools import FlinkStatementLLMTools
from flink_skill_common.deploy.statements import ddl_statement_name, dml_statement_name
from flink_skill_common.llm import llm_reachable, resolve_llm_model

from flink_skill_common.config import (
    agent_fixer_max_retries,
    llm_api_key,
    llm_base_url,
    load_env,
    skill_dir,
)


def _make_model():
    return make_openai_model(
        base_url=llm_base_url(),
        api_key=llm_api_key(),
        model_id=resolve_llm_model(),
    )


def build_ksql_migrate_agent():
    """Create Agno agent with ksql-to-flink skill loaded from skill/."""
    return build_migration_agent(
        name="KsqlToFlinkAgent",
        skill_dir=skill_dir(),
        instructions=[
            "Migrate one ksqlDB CREATE STREAM/TABLE statement at a time to Confluent Cloud Flink SQL.",
            "Call get_skill_instructions('ksql-to-flink') before translating.",
            "Input is a single CREATE (with optional CSAS body), not a multi-statement script.",
            "Apply translation rules from skill references as needed.",
            "Never output CREATE STREAM in DDL; use CREATE TABLE IF NOT EXISTS only.",
            "Return final DDL and DML as separate labeled ```sql fenced blocks (DDL first, then DML).",
            "Do not include explanations outside the SQL blocks.",
        ],
        model=_make_model(),
        tools=[]
    )


def build_deploy_retry_agent():
    """Agent with confluent-sql tools for fixing failed Flink deploys."""
    deploy_tools = FlinkStatementLLMTools()
    return build_migration_agent(
        name="KsqlToFlinkDeployAgent",
        skill_dir=skill_dir(),
        instructions=[
            "Fix Flink SQL deploy failures for ksql-to-flink migrations.",
            "Read skill reference confluent-sql-deploy.md for deploy tool sequence.",
            "Deploy source stub DDLs from tests/ddl.*.sql before target DDL and DML.",
            "Use create_flink_statement to redeploy source DDLs, then target DDL, then DML.",
            "Use get_flink_statement_exceptions to understand failures.",
            "Use list_flink_statements or get_flink_statement to verify RUNNING phase.",
            "Statement names: {table}-ddl and {table}-dml with underscores replaced by hyphens.",
            "If DML fails because a source table is missing, regenerate stub DDL in tests/ via LLM.",
        ],
        model=_make_model(),
        tools=deploy_tools.as_tools(),
    )


def migrate_prompt(table_name: str, ksql: str, *, source_name: str | None = None) -> str:
    """Build a structured migration request for the agent."""
    source = source_name or "the ksql object in this statement"
    return (
        f"Migrate the following single ksqlDB CREATE statement to Flink SQL.\n"
        f"Target Flink table name: `{table_name}`.\n"
        f"ksql object in this statement: `{source}`.\n\n"
        f"Follow the ksql-to-flink skill workflow: translate only this one CREATE "
        f"(stream/table definition and any CSAS query in the same statement). "
        f"Do not assume other CREATE statements from the same file are in scope.\n\n"
        f"```sql\n{ksql.strip()}\n```"
    )


def deploy_retry_prompt(
    table_name: str,
    ksql: str,
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
        source_files = sorted(tests_dir.glob("ddl.*.sql"))
        if source_files:
            blocks = []
            for path in source_files:
                blocks.append(f"{path.name}:\n```sql\n{path.read_text().strip()}\n```")
            source_section = (
                "Source stub DDLs (tests/ — deploy these before target DDL/DML):\n"
                + "\n\n".join(blocks)
                + "\n\n"
            )

    return (
        f"Flink deploy failed for table `{table_name}`.\n\n"
        f"Error:\n{error_message}\n\n"
        f"Target statement names: DDL `{ddl_stmt}`, DML `{dml_stmt}`.\n\n"
        f"Original ksql:\n```sql\n{ksql.strip()}\n```\n\n"
        f"{source_section}"
        f"Current DDL ({ddl_path}):\n```sql\n{ddl.strip()}\n```\n\n"
        f"Current DML ({dml_path}):\n```sql\n{dml.strip()}\n```\n\n"
        "1. Call get_flink_statement_exceptions for the failed statement.\n"
        "2. Fix source stub DDLs in tests/ if missing-table errors; then target DDL/DML.\n"
        "3. Redeploy via create_flink_statement (source DDLs, target DDL, then DML).\n"
        "4. Confirm RUNNING with get_flink_statement or list_flink_statements.\n"
        "Return corrected source DDLs, DDL, and DML in labeled ```sql blocks."
    )


def run_migration(table_name: str, ksql: str, *, source_name: str | None = None) -> str:
    """Run migration agent and return response content."""
    agent = build_ksql_migrate_agent()
    return run_agent_response(agent, migrate_prompt(table_name, ksql, source_name=source_name))


def run_agent_deploy_retry(
    table_name: str,
    ksql: str,
    ddl_path: Path,
    dml_path: Path,
    error_message: str,
    tests_dir: Path | None = None,
) -> str:
    """Invoke Agno agent with confluent-sql tools to fix and redeploy failed statements."""
    agent = build_deploy_retry_agent()
    prompt = deploy_retry_prompt(
        table_name, ksql, ddl_path, dml_path, error_message, tests_dir=tests_dir
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


def main() -> None:
    load_env()
    if not llm_reachable():
        print("LLM not reachable. Start oMLX or set SL_LLM_BASE_URL.", file=sys.stderr)
        sys.exit(1)
    prompt = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "List the ksql-to-flink migration workflow."
    agent = build_ksql_migrate_agent()
    resp= run_agent_response(agent, prompt)
    print(resp)


if __name__ == "__main__":
    main()
