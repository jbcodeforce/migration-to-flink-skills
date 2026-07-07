"""Offline CLI tests for spark migrate command."""

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from flink_skill_common.convergence import ConvergenceResult
from spark_flink_skill.cli import app

runner = CliRunner()


def test_migrate_calls_convergence_after_translation(tmp_path: Path):
    spark_file = tmp_path / "test.sql"
    spark_file.write_text("CREATE TABLE src (id INT);")
    out_dir = tmp_path / "out"
    converge_result = ConvergenceResult(
        success=True,
        ddls=["CREATE TABLE IF NOT EXISTS t (id INT);"],
        dmls=["INSERT INTO t SELECT id FROM src;"],
        ddl_path=out_dir / "t" / "ddl.t.sql",
        dml_path=out_dir / "t" / "dml.t.sql",
        messages=["Offline validation passed."],
    )

    with (
        patch("spark_flink_skill.cli.llm_reachable", return_value=True),
        patch("spark_flink_skill.cli.resolve_llm_model", return_value="test-model"),
        patch(
            "spark_flink_skill.cli.run_migration",
            return_value="```sql\nCREATE TABLE t (id INT);\n```\n```sql\nINSERT INTO t SELECT id FROM src;\n```",
        ),
        patch(
            "spark_flink_skill.cli.clean_flink_sql_and_validate",
            return_value=converge_result,
        ) as mock_converge,
    ):
        result = runner.invoke(
            app,
            [
                "--table",
                "my_table",
                "--file",
                str(spark_file),
                "--out-dir",
                str(out_dir),
                "--skip-deploy",
            ],
        )

    assert result.exit_code == 0, result.output
    mock_converge.assert_called_once()
    assert mock_converge.call_args.args[3] is True
    assert "Offline validation passed" in result.output


def test_migrate_exits_1_when_convergence_fails(tmp_path: Path):
    spark_file = tmp_path / "test.sql"
    spark_file.write_text("CREATE TABLE src (id INT);")
    out_dir = tmp_path / "out"
    converge_result = ConvergenceResult(
        success=False,
        ddls=[],
        dmls=[],
        ddl_path=None,
        dml_path=None,
        messages=["Offline validation failed"],
    )

    with (
        patch("spark_flink_skill.cli.llm_reachable", return_value=True),
        patch("spark_flink_skill.cli.resolve_llm_model", return_value="test-model"),
        patch("spark_flink_skill.cli.run_migration", return_value="bad sql"),
        patch(
            "spark_flink_skill.cli.clean_flink_sql_and_validate",
            return_value=converge_result,
        ),
    ):
        result = runner.invoke(
            app,
            [
                "--table",
                "my_table",
                "--file",
                str(spark_file),
                "--out-dir",
                str(out_dir),
            ],
        )

    assert result.exit_code == 1
    assert "Offline validation failed" in result.output


def test_migrate_exits_130_on_keyboard_interrupt(tmp_path: Path):
    spark_file = tmp_path / "test.sql"
    spark_file.write_text("CREATE TABLE src (id INT);")
    out_dir = tmp_path / "out"

    with (
        patch("spark_flink_skill.cli.llm_reachable", return_value=True),
        patch("spark_flink_skill.cli.resolve_llm_model", return_value="test-model"),
        patch(
            "spark_flink_skill.cli.run_migration",
            side_effect=KeyboardInterrupt,
        ),
    ):
        result = runner.invoke(
            app,
            [
                "--table",
                "my_table",
                "--file",
                str(spark_file),
                "--out-dir",
                str(out_dir),
            ],
        )

    assert result.exit_code == 130
    assert "Migration interrupted" in result.output
