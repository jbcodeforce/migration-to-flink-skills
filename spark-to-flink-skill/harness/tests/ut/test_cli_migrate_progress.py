"""Offline CLI tests for migrate command progress output."""

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from flink_skill_common.convergence import ConvergenceResult
from spark_to_flink.cli import app

runner = CliRunner()

VALID_DDL = """CREATE TABLE IF NOT EXISTS customers (
    id INT
) DISTRIBUTED BY HASH(id) INTO 1 BUCKETS
WITH ('changelog.mode' = 'append');"""

VALID_DML = "INSERT INTO customers SELECT id FROM src;"


def test_migrate_verbose_progress_with_mocks(tmp_path: Path):
    spark_file = tmp_path / "input.sql"
    spark_file.write_text("CREATE TABLE customers (id INT);")
    out_dir = tmp_path / "output"
    table_dir = out_dir / "customers"
    agent_response = f"DDL\n```sql\n{VALID_DDL}\n```\nDML\n```sql\n{VALID_DML}\n```"
    convergence_result = ConvergenceResult(
        success=True,
        ddls=[VALID_DDL],
        dmls=[VALID_DML],
        ddl_path=table_dir / "ddl.customers.sql",
        dml_path=table_dir / "dml.customers.sql",
        messages=["Offline validation passed.", "Skipped deploy (--skip-deploy)."],
    )

    def _fake_converge(*args, **kwargs):
        on_progress = kwargs.get("on_progress")
        if on_progress:
            on_progress("Running offline validation...")
            on_progress("Offline validation passed.")
        return convergence_result

    with (
        patch("spark_to_flink.cli.llm_reachable", return_value=True),
        patch("spark_to_flink.cli.resolve_llm_model", return_value="test-model"),
        patch("spark_to_flink.cli.run_migration", return_value=agent_response),
        patch(
            "flink_skill_common.convergence.compute_missing_source_tables",
            return_value=[],
        ),
        patch(
            "flink_skill_common.convergence.converge_flink_sql",
            side_effect=_fake_converge,
        ) as mock_converge,
    ):
        result = runner.invoke(
            app,
            [
                "--table",
                "customers",
                "--file",
                str(spark_file),
                "--out-dir",
                str(out_dir),
                "--skip-deploy",
            ],
        )

    assert result.exit_code == 0, result.output
    output = result.output
    assert "migrate to flink CLI" in output
    assert "test-model" in output
    assert "Found 1 CREATE statement(s)" in output
    assert "Wrote statement files to input.statements/" in output
    assert "customers → customers" in output
    assert "Running translation agent" in output
    assert "Extracted 1 DDL, 1 DML" in output
    assert "Running offline validation" in output
    assert "Offline validation passed" in output
    assert mock_converge.call_args.kwargs["on_progress"] is not None
    assert (tmp_path / "input.statements" / "manifest.json").is_file()
