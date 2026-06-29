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
from collections.abc import Callable

from flink_skill_common.agents.factory import (
    build_migration_agent,
    make_openai_model,
    run_agent_response,
)
from flink_skill_common.llm import llm_reachable, resolve_llm_model

from flink_skill_common.config import (
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
            "Apply translation rules from skill references as needed.",
         ],
        model=_make_model(),
        tools=[]
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


def run_migration(
    table_name: str,
    ksql: str,
    *,
    source_name: str | None = None,
    on_event: Callable[[str], None] | None = None,
) -> str:
    """Run migration agent and return response content."""
    agent = build_ksql_migrate_agent()
    return run_agent_response(
        agent,
        migrate_prompt(table_name, ksql, source_name=source_name),
        on_event=on_event,
    )


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
