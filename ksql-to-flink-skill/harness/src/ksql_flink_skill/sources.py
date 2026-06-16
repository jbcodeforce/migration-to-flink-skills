"""Generate stub DDL for DML source tables via LLM."""

from __future__ import annotations

from agno.agent import Agent

from flink_skill_common.agents.factory import make_openai_model
from flink_skill_common.llm import resolve_llm_model

from ksql_flink_skill.config import llm_api_key, llm_base_url, load_env, skill_dir
from ksql_flink_skill.output import parse_source_ddls_from_response


def _make_model():
    load_env()
    return make_openai_model(
        base_url=llm_base_url(),
        api_key=llm_api_key(),
        model_id=resolve_llm_model(),
    )


def _source_ddl_prompt_template() -> str:
    path = skill_dir() / "prompts/ksql_fsql/source_ddl.txt"
    return path.read_text()


def source_ddl_prompt(
    target_table: str,
    ksql: str,
    dml_sql: str,
    missing_sources: list[str],
) -> str:
    """Build prompt for LLM source DDL generation."""
    sources_list = ", ".join(missing_sources)
    return (
        f"{_source_ddl_prompt_template()}\n\n"
        f"target_table: {target_table}\n"
        f"missing_sources: [{sources_list}]\n\n"
        f"ksql_script:\n```sql\n{ksql.strip()}\n```\n\n"
        f"dml_sql:\n```sql\n{dml_sql.strip()}\n```"
    )


def build_source_ddl_agent() -> Agent:
    """Agent for generating source table DDL stubs (no MCP)."""
    return Agent(
        name="SourceDdlAgent",
        model=_make_model(),
        instructions=[
            "Generate Flink CREATE TABLE IF NOT EXISTS DDL stubs for upstream source tables.",
            "Follow the JSON output format in the user prompt exactly.",
            "Respond with JSON only — no markdown fences or explanations.",
        ],
    )


def generate_source_ddls(
    target_table: str,
    ksql: str,
    dml_sql: str,
    missing_sources: list[str],
) -> dict[str, str]:
    """Call LLM to produce stub DDL for each missing source table."""
    if not missing_sources:
        return {}

    agent = build_source_ddl_agent()
    prompt = source_ddl_prompt(target_table, ksql, dml_sql, missing_sources)
    response = agent.run(prompt)
    content = str(response.content) if hasattr(response, "content") else str(response)
    parsed = parse_source_ddls_from_response(content)

    result: dict[str, str] = {}
    for name in missing_sources:
        ddl = parsed.get(name) or parsed.get(name.lower())
        if ddl:
            result[name] = ddl

    missing_after = [n for n in missing_sources if n not in result and n.lower() not in {k.lower() for k in result}]
    if missing_after:
        raise ValueError(
            f"LLM did not return DDL for source tables: {', '.join(missing_after)}"
        )
    return result
