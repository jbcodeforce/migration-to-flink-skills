"""Live LLM migration IT for src_customer_profiles — dumps prompt/response for tracing."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from flink_skill_common.curated_mappings import build_curated_context_block
from spark_ref_fixtures import SparkMigrateCase, spark_source_path
from spark_to_flink.migrate_agent import (
    _migrate_prompt,
    build_spark_migrate_agent,
    run_migration,
)
from spark_to_flink.output import extract_sql_blocks
from spark_to_flink.sql_utils import (
    clean_sql_input,
    extract_spark_object_name,
    split_sql_create_statements,
)

pytestmark = pytest.mark.integration

_CASE = SparkMigrateCase(
    "tables/src_customer_profiles.sql", "src_customer_profiles", "tables"
)


def _source_path() -> Path:
    return spark_source_path(_CASE)


def _agent_with_debug(seen: dict):
    agent = build_spark_migrate_agent()
    agent.debug_mode = True
    agent.debug_level = 2
    seen["agent"] = agent
    seen["debug_mode"] = getattr(agent, "debug_mode", None)
    seen["debug_level"] = getattr(agent, "debug_level", None)
    return agent


def test_live_migrate_src_customer_profiles_dumps_llm_exchange(
    require_llm, tmp_path: Path
):
    src_file = _source_path()
    if not src_file.is_file():
        pytest.skip(f"Missing source: {src_file}")

    raw = src_file.read_text()
    cleaned = clean_sql_input(raw)
    statements = split_sql_create_statements(cleaned)
    spark_sql = statements[0] if statements else cleaned
    table_name = extract_spark_object_name(spark_sql) or "src_customer_profiles"

    curated = build_curated_context_block(
        table_name=table_name,
        ksql=spark_sql,
        src_ksql=cleaned,
        src_file=src_file,
    )
    prompt = _migrate_prompt(
        table_name=table_name,
        spark_sql=spark_sql,
        src_spark=cleaned,
        source_name=table_name,
        curated_context=curated,
    )

    prompt_path = tmp_path / "prompt.txt"
    prompt_path.write_text(prompt)
    print("\n===== LLM PROMPT (also written to prompt.txt) =====\n")
    print(prompt)
    print("\n===== END PROMPT =====\n")

    captured: dict = {}

    def _capture_run(agent, prompt_text, *, on_event=None):
        captured["prompt"] = prompt_text
        from flink_skill_common.agents.factory import run_agent_process_response

        return run_agent_process_response(agent, prompt_text, on_event=on_event)

    with (
        patch(
            "spark_to_flink.migrate_agent.build_spark_migrate_agent",
            side_effect=lambda: _agent_with_debug(captured),
        ),
        patch(
            "spark_to_flink.migrate_agent.run_agent_process_response",
            side_effect=_capture_run,
        ),
    ):
        response = run_migration(
            table_name=table_name,
            spark_sql=spark_sql,
            src_spark=cleaned,
            source_name=table_name,
            src_file=src_file,
            on_event=lambda msg: print(f"[agent] {msg}"),
        )

    response_path = tmp_path / "response.txt"
    response_path.write_text(response)
    print("\n===== LLM RESPONSE (also written to response.txt) =====\n")
    print(response)
    print("\n===== END RESPONSE =====\n")
    print(f"Artifacts: {prompt_path} ({prompt_path.stat().st_size} bytes)")
    print(f"Artifacts: {response_path} ({response_path.stat().st_size} bytes)")
    print(
        f"Agent debug_mode={captured.get('debug_mode')!r} "
        f"debug_level={captured.get('debug_level')!r}"
    )

    assert captured.get("debug_mode") is True, (
        "Agno debug_mode attribute set failed; "
        "may need build_spark_migrate_agent(debug_mode=...) kwargs"
    )
    assert captured.get("debug_level") == 2
    assert captured.get("prompt"), "expected prompt passed to agent run"
    assert "Statement to migrate:" in captured["prompt"]
    assert response.strip(), "expected non-empty LLM response"
    ddls, _dmls = extract_sql_blocks(response)
    assert ddls, "expected at least one DDL sql block in response"
    assert any("CREATE TABLE" in d.upper() for d in ddls), ddls
