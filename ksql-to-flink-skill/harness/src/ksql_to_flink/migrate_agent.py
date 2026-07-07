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

from collections.abc import Callable

import ksql_to_flink.config  # noqa: F401 — configure shared harness context
from flink_skill_common.agents.factory import (
    build_skilled_agent,
    make_openai_model,
    run_agent_process_response,
)
from flink_skill_common.agents.factory import resolve_llm_model

from flink_skill_common.config import (
    llm_api_key,
    llm_base_url,
    skill_dir,
)


def _make_model():
    return make_openai_model(
        base_url=llm_base_url(),
        api_key=llm_api_key(),
        model_id=resolve_llm_model(),
    )


def build_ksql_migrate_agent():
    """Create Agno agent with ksql-to-flink translation skill only."""
    return build_skilled_agent(
        name="KsqlToFlinkAgent",
        skill_dirs=[skill_dir()],
        instructions=[
            "Migrate one ksqlDB CREATE STREAM/TABLE statement at a time to Confluent Cloud Flink SQL.",
            "Call get_skill_instructions('ksql-to-flink') before translating.",
            "Return DDL and DML as separate labeled ```sql fenced blocks (DDL first, then DML).",
            "Do not validate, deploy, or generate source stub DDL — the harness runs convergence after translation.",
        ],
        model=_make_model(),
        tools=[],
    )


def _migrate_prompt(
    table_name: str,
    ksql: str,
    src_ksql: str,
    source_name: str | None = None) -> str:
    """Build a structured migration request for the agent."""
    source = source_name or "the ksql object in this statement"
    return (
        f"Migrate the following single ksqlDB CREATE statement to Flink SQL.\n"
        f"Target Flink table name: `{table_name}`.\n"
        f"ksql object in this statement: `{source}`.\n\n"
        f"Follow the ksql-to-flink skill workflow: translate only this one CREATE "
        f"(stream/table definition and any CSAS query in the same statement). "
        f"Return DDL and DML in labeled ```sql blocks.\n\n"
        f"Full ksql script (for upstream table names and schemas):\n"
        f"```sql\n{src_ksql.strip()}\n```\n\n"
        f"The harness validates and deploys after translation — do not validate or deploy yourself.\n\n"
        f"Statement to migrate:\n"
        f"```sql\n{ksql.strip()}\n```"
    )


def run_migration(
    table_name: str,
    ksql: str,
    src_ksql: str,
    *,
    source_name: str | None = None,
    on_event: Callable[[str], None] | None = None,
) -> str:
    """Run migration agent and return response content."""
    agent = build_ksql_migrate_agent()
    return run_agent_process_response(
        agent,
        _migrate_prompt(
            table_name=table_name, 
            ksql=ksql, 
            src_ksql=src_ksql, 
            source_name=source_name),
        on_event=on_event,
    )
