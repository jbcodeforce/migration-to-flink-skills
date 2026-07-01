"""Agno agent with spark-to-flink skill for Spark SQL migration."""

from __future__ import annotations

from flink_skill_common.agents.factory import (
    build_migration_agent,
    make_openai_model,
    run_agent_response,
)
from flink_skill_common.llm import is_agent_error_response, resolve_llm_model

from spark_flink_skill.config import llm_api_key, llm_base_url, load_env, skill_dir


def _make_model():
    load_env()
    return make_openai_model(
        base_url=llm_base_url(),
        api_key=llm_api_key(),
        model_id=resolve_llm_model(),
    )


def build_migrate_agent():
    """Create Agno agent with spark-to-flink skill loaded from skill/."""
    instructions = [
        "Migrate Spark SQL to Confluent Cloud Flink SQL using the spark-to-flink skill.",
        "Call get_skill_instructions('spark-to-flink') before translating.",
        "Apply translation and validation rules before returning output.",
        "Return final DDL and DML as separate labeled ```sql fenced blocks (DDL first, then DML).",
        "Do not include explanations outside the SQL blocks.",
    ]
    return build_migration_agent(
        name="SparkToFlinkAgent",
        skill_dir=skill_dir(),
        instructions=instructions,
        model=_make_model(),
    )


def migrate_prompt(table_name: str, spark_sql: str) -> str:
    """Build a structured migration request for the agent."""
    return (
        f"Migrate the following Spark SQL to Flink SQL for table `{table_name}`.\n\n"
        f"Follow the spark-to-flink skill workflow: translate, validate, then output DDL and DML.\n\n"
        f"```sql\n{spark_sql.strip()}\n```"
    )


class MigrationError(RuntimeError):
    """Raised when the agent fails to produce migration output."""


def run_migration(table_name: str, spark_sql: str) -> str:
    """Run migration agent and return response content."""
    agent = build_migrate_agent()
    content = run_agent_response(agent, migrate_prompt(table_name, spark_sql))
    if is_agent_error_response(content):
        raise MigrationError(content.strip() or "Agent returned no migration output.")
    return content
