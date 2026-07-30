"""Unit tests for migrate prompt curated context wiring."""

from __future__ import annotations

from unittest.mock import patch

from spark_to_flink.migrate_agent import _migrate_prompt, run_migration


def test_migrate_prompt_includes_curated_section():
    prompt = _migrate_prompt(
        table_name="src_customers",
        spark_sql="CREATE OR REPLACE TEMPORARY VIEW src_customers AS SELECT 1;",
        src_spark="CREATE OR REPLACE TEMPORARY VIEW src_customers AS SELECT 1;",
        source_name="src_customers",
        curated_context=(
            "## Curated Flink reference (same pattern — follow shape, adapt names/columns)\n"
            "### Exact curated pipeline\n"
            "```sql\nCREATE TABLE IF NOT EXISTS src_c360_customers (id STRING);\n```"
        ),
    )
    assert "Curated Flink reference" in prompt
    assert "Statement to migrate:" in prompt
    assert "src_customers" in prompt


def test_migrate_prompt_omits_curated_when_empty():
    prompt = _migrate_prompt(
        table_name="t",
        spark_sql="CREATE TABLE t (id INT);",
        src_spark="CREATE TABLE t (id INT);",
        curated_context="",
    )
    assert "Curated Flink reference" not in prompt


def test_run_migration_passes_curated_into_prompt():
    captured: dict[str, str] = {}

    def fake_run(agent, prompt, *, on_event=None):
        captured["prompt"] = prompt
        return "ok"

    with patch(
        "spark_to_flink.migrate_agent.build_curated_context_block",
        return_value="## Curated Flink reference\n```sql\nSELECT 1;\n```",
    ):
        with patch(
            "spark_to_flink.migrate_agent.build_spark_migrate_agent",
            return_value=object(),
        ):
            with patch(
                "spark_to_flink.migrate_agent.run_agent_process_response",
                side_effect=fake_run,
            ):
                result = run_migration(
                    table_name="src_customers",
                    spark_sql="CREATE TABLE src_customers (id INT);",
                    src_spark="CREATE TABLE src_customers (id INT);",
                    src_file="references/spark/c360/sources/src_customers.sql",
                )
    assert result == "ok"
    assert "Curated Flink reference" in captured["prompt"]
