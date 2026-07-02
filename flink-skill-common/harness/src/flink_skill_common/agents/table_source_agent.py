"""
Copyright 2024-2026 Confluent, Inc.
KSQL to Flink SQL Translation Agent


Generate stub DDL for DML source tables via LLM.
"""

from __future__ import annotations

from agno.agent import Agent

from flink_skill_common.agents.factory import make_openai_model, resolve_llm_model, build_skilled_agent

from flink_skill_common.config import (
    flink_skill_common_skill_dir,
    llm_api_key,
    llm_base_url,
    load_env,
)
from flink_skill_common.response_io import parse_source_ddls_from_response


def _make_model():
    load_env()
    return make_openai_model(
        base_url=llm_base_url(),
        api_key=llm_api_key(),
        model_id=resolve_llm_model(),
    )


def _source_ddl_prompt_template() -> str:
    path = flink_skill_common_skill_dir() / "prompts/source_ddl.txt"
    return path.read_text()


def _source_ddl_prompt(
    target_table: str,
    src_sql: str,
    dml_sql: str,
    missing_sources: list[str],
) -> str:
    """Build prompt for LLM source DDL generation."""
    sources_list = ", ".join(missing_sources)
    return (
        f"{_source_ddl_prompt_template()}\n\n"
        f"target_table: {target_table}\n"
        f"missing_sources: [{sources_list}]\n\n"
        f"sql_script:\n```sql\n{src_sql.strip()}\n```\n\n"
        f"dml_sql:\n```sql\n{dml_sql.strip()}\n```"
    )


def generate_source_ddls(
    target_table: str,
    src_sql: str,
    dml_sql: str,
    missing_sources: list[str],
) -> dict[str, str]:
    """Call LLM to produce stub DDL for each missing source table."""
    if not missing_sources:
        return {}

    agent = build_skilled_agent(
        name="SourceDdlAgent",
        skill_dir=flink_skill_common_skill_dir(),
        model=_make_model(),
        instructions=[
            "Generate Flink CREATE TABLE IF NOT EXISTS DDL stubs for upstream source tables.",
            "Follow the JSON output format in the user prompt exactly.",
            "Respond with JSON only — no markdown fences or explanations.",
        ],
    )
    prompt = _source_ddl_prompt(target_table, src_sql, dml_sql, missing_sources)
    response = agent.run(prompt)
    content = str(response.content) if hasattr(response, "content") else str(response)
    parsed_ddls = parse_source_ddls_from_response(content)

    result: dict[str, str] = {}
    for name in missing_sources:
        ddl = parsed_ddls.get(name) or parsed_ddls.get(name.lower())
        if ddl:
            result[name] = ddl

    missing_after = [n for n in missing_sources if n not in result and n.lower() not in {k.lower() for k in result}]
    if missing_after:
        raise ValueError(
            f"LLM did not return DDL for source tables: {', '.join(missing_after)}"
        )
    return result
