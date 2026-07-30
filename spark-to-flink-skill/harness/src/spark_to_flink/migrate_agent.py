"""Agno agent with spark-to-flink skill for Spark SQL migration."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from flink_skill_common.agents.factory import (
    build_skilled_agent,
    make_openai_model,
    resolve_llm_model,
    run_agent_process_response,
)
from flink_skill_common.config import llm_api_key, llm_base_url, skill_dir
from flink_skill_common.curated_mappings import build_curated_context_block


def build_spark_migrate_agent():
    """Create Agno agent with spark-to-flink translation skill only."""
    return build_skilled_agent(
        name="SparkToFlinkAgent",
        skill_dirs=[skill_dir()],
        instructions=[
            "Migrate one Spark SQL CREATE TABLE/VIEW statement at a time to Confluent Cloud Flink SQL.",
            "Call get_skill_instructions('spark-to-flink') before translating.",
            "Return DDL and DML as separate labeled ```sql fenced blocks (DDL first, then DML).",
            "When a curated Flink reference is present in the prompt, match its PRIMARY KEY, "
            "join, and changelog shape; adapt names/columns to this statement; serdes may differ.",
        ],
        model=make_openai_model(
            base_url=llm_base_url(),
            api_key=llm_api_key(),
            model_id=resolve_llm_model(),
        ),
        tools=[],
    )


def _migrate_prompt(
    table_name: str,
    spark_sql: str,
    src_spark: str,
    source_name: str | None = None,
    curated_context: str = "",
) -> str:
    """Build a structured migration request for the agent."""
    source = source_name or "the Spark object in this statement"
    curated = ""
    if curated_context.strip():
        curated = f"\n\n{curated_context.strip()}\n"
    return (
        f"Migrate the following single Spark SQL CREATE statement to Flink SQL.\n"
        f"Target Flink table name: `{table_name}`.\n"
        f"Spark object in this statement: `{source}`.\n\n"
        f"Follow the spark-to-flink skill workflow: translate only this one CREATE "
        f"(table/view definition and its query). "
        f"Return DDL and DML in labeled ```sql blocks.\n\n"
        f"Full Spark script (for upstream table names and schemas):\n"
        f"```sql\n{src_spark.strip()}\n```\n\n"
        f"The harness validates and deploys after translation — do not validate or deploy yourself."
        f"{curated}\n"
        f"Statement to migrate:\n"
        f"```sql\n{spark_sql.strip()}\n```"
    )


def run_migration(
    table_name: str,
    spark_sql: str,
    src_spark: str,
    *,
    source_name: str | None = None,
    src_file: Path | str | None = None,
    category: str | None = None,
    on_event: Callable[[str], None] | None = None,
) -> str:
    """Run migration agent and return response content."""
    curated = build_curated_context_block(
        table_name=table_name,
        ksql=spark_sql,
        src_ksql=src_spark,
        src_file=src_file,
        category=category,
    )
    agent = build_spark_migrate_agent()
    return run_agent_process_response(
        agent,
        _migrate_prompt(
            table_name=table_name,
            spark_sql=spark_sql,
            src_spark=src_spark,
            source_name=source_name,
            curated_context=curated,
        ),
        on_event=on_event,
    )
