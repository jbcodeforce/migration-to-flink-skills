"""Unit tests for migrate prompt curated context wiring."""

from __future__ import annotations

from unittest.mock import patch

from ksql_to_flink.migrate_agent import _migrate_prompt, run_migration


def test_migrate_prompt_includes_curated_section():
    prompt = _migrate_prompt(
        table_name="orders_enriched",
        ksql="CREATE STREAM orders_enriched AS SELECT 1;",
        src_ksql="CREATE STREAM orders_enriched AS SELECT 1;",
        source_name="orders_enriched",
        curated_context=(
            "## Curated Flink reference (same pattern — follow shape, adapt names/columns)\n"
            "### Exact curated pipeline\n"
            "```sql\nCREATE TABLE IF NOT EXISTS orders_enriched (id STRING);\n```"
        ),
    )
    print(prompt)
    assert "Curated Flink reference" in prompt
    assert "Statement to migrate:" in prompt
    assert "orders_enriched" in prompt


def test_migrate_prompt_omits_curated_when_empty():
    prompt = _migrate_prompt(
        table_name="t",
        ksql="CREATE STREAM t (id INT);",
        src_ksql="CREATE STREAM t (id INT);",
        curated_context="",
    )
    assert "Curated Flink reference" not in prompt


def test_run_migration_passes_curated_into_prompt():
    captured: dict[str, str] = {}

    def fake_run(agent, prompt, *, on_event=None):
        captured["prompt"] = prompt
        return "ok"

    with patch(
        "ksql_to_flink.migrate_agent.build_curated_context_block",
        return_value="## Curated Flink reference\n```sql\nSELECT 1;\n```",
    ):
        with patch(
            "ksql_to_flink.migrate_agent.build_ksql_migrate_agent",
            return_value=object(),
        ):
            with patch(
                "ksql_to_flink.migrate_agent.run_agent_process_response",
                side_effect=fake_run,
            ):
                result = run_migration(
                    table_name="orders_enriched",
                    ksql="CREATE STREAM orders_enriched AS SELECT 1;",
                    src_ksql="CREATE STREAM orders_enriched AS SELECT 1;",
                    src_file="references/ksql/sources/joins/multi-joins.ksql",
                )
    assert result == "ok"
    assert "Curated Flink reference" in captured["prompt"]


