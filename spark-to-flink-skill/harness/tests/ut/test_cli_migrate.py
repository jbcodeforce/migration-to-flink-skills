"""Offline CLI tests for spark migrate command deploy wiring and manifest resume."""

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from flink_skill_common.convergence import ConvergenceResult
from flink_skill_common.migration_manifest import load_manifest, statements_dir_for
from spark_to_flink.cli import app

runner = CliRunner()


def test_migrate_exits_130_on_keyboard_interrupt(tmp_path: Path):
    spark_file = tmp_path / "test.sql"
    spark_file.write_text("CREATE TABLE src (id INT);")
    out_dir = tmp_path / "out"

    with (
        patch("spark_to_flink.cli.llm_reachable", return_value=True),
        patch("spark_to_flink.cli.resolve_llm_model", return_value="test-model"),
        patch("spark_to_flink.cli.run_migration", side_effect=KeyboardInterrupt),
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
    assert result.exit_code == 130
    assert "Interrupted during src" in result.output
    manifest = load_manifest(statements_dir_for(spark_file))
    assert manifest is not None
    assert manifest.statements[0].status == "interrupted"


def test_migrate_writes_statement_files_and_marks_migrated(tmp_path: Path):
    spark_file = tmp_path / "pipeline.sql"
    spark_file.write_text(
        "CREATE TABLE clicks (id INT);\n"
        "CREATE OR REPLACE TEMPORARY VIEW detected AS SELECT * FROM clicks;\n"
    )
    out_dir = tmp_path / "out"
    calls: list[str] = []

    def fake_migration(*, table_name: str, spark_sql: str, **_kwargs):
        calls.append(table_name)
        return f"```sql\nCREATE TABLE IF NOT EXISTS {table_name} (id INT);\n```"

    with (
        patch("spark_to_flink.cli.llm_reachable", return_value=True),
        patch("spark_to_flink.cli.resolve_llm_model", return_value="test-model"),
        patch("spark_to_flink.cli.run_migration", side_effect=fake_migration),
        patch("spark_to_flink.cli.clean_flink_sql_and_validate", return_value=None),
    ):
        result = runner.invoke(
            app,
            [
                "--file",
                str(spark_file),
                "--out-dir",
                str(out_dir),
                "--skip-deploy",
            ],
        )
    assert result.exit_code == 0, result.output
    assert calls == ["clicks", "detected"]
    statements_dir = statements_dir_for(spark_file)
    assert (statements_dir / "001_clicks.sql").is_file()
    assert (statements_dir / "002_detected.sql").is_file()
    manifest = load_manifest(statements_dir)
    assert manifest is not None
    assert all(e.status == "migrated" for e in manifest.statements)


def test_migrate_resume_skips_migrated(tmp_path: Path):
    spark_file = tmp_path / "pipeline.sql"
    text = (
        "CREATE TABLE clicks (id INT);\n"
        "CREATE OR REPLACE TEMPORARY VIEW detected AS SELECT * FROM clicks;\n"
    )
    spark_file.write_text(text)
    out_dir = tmp_path / "out"
    calls: list[str] = []

    def fake_migration(*, table_name: str, **_kwargs):
        calls.append(table_name)
        return f"```sql\nCREATE TABLE IF NOT EXISTS {table_name} (id INT);\n```"

    with (
        patch("spark_to_flink.cli.llm_reachable", return_value=True),
        patch("spark_to_flink.cli.resolve_llm_model", return_value="test-model"),
        patch("spark_to_flink.cli.run_migration", side_effect=fake_migration),
        patch("spark_to_flink.cli.clean_flink_sql_and_validate", return_value=None),
    ):
        first = runner.invoke(
            app,
            ["--file", str(spark_file), "--out-dir", str(out_dir), "--skip-deploy"],
        )
        assert first.exit_code == 0, first.output
        calls.clear()
        with patch("spark_to_flink.cli.split_sql_create_statements") as mock_split:
            second = runner.invoke(
                app,
                ["--file", str(spark_file), "--out-dir", str(out_dir), "--skip-deploy"],
            )
    assert second.exit_code == 0, second.output
    assert calls == []
    assert "already migrated" in second.output
    assert "split skipped" in second.output
    mock_split.assert_not_called()


def test_migrate_failure_marks_failed_leaves_later_pending(tmp_path: Path):
    spark_file = tmp_path / "pipeline.sql"
    spark_file.write_text(
        "CREATE TABLE clicks (id INT);\n"
        "CREATE OR REPLACE TEMPORARY VIEW detected AS SELECT * FROM clicks;\n"
    )
    out_dir = tmp_path / "out"
    failure = ConvergenceResult(
        success=False,
        ddls=["CREATE TABLE clicks (id INT)"],
        dmls=["INSERT INTO clicks SELECT 1"],
        ddl_path=None,
        dml_path=None,
        messages=["Validation failed: bad sql"],
    )

    with (
        patch("spark_to_flink.cli.llm_reachable", return_value=True),
        patch("spark_to_flink.cli.resolve_llm_model", return_value="test-model"),
        patch(
            "spark_to_flink.cli.run_migration",
            return_value="```sql\nCREATE TABLE clicks (id INT);\n```",
        ),
        patch("spark_to_flink.cli.clean_flink_sql_and_validate", return_value=failure),
    ):
        result = runner.invoke(
            app,
            [
                "--file",
                str(spark_file),
                "--out-dir",
                str(out_dir),
                "--skip-deploy",
            ],
        )
    assert result.exit_code == 1
    manifest = load_manifest(statements_dir_for(spark_file))
    assert manifest is not None
    assert manifest.statements[0].status == "failed"
    assert manifest.statements[1].status == "pending"


def test_migrate_resume_retries_failed_only(tmp_path: Path):
    spark_file = tmp_path / "pipeline.sql"
    spark_file.write_text(
        "CREATE TABLE clicks (id INT);\n"
        "CREATE OR REPLACE TEMPORARY VIEW detected AS SELECT * FROM clicks;\n"
    )
    out_dir = tmp_path / "out"
    failure = ConvergenceResult(
        success=False,
        ddls=["ddl"],
        dmls=["dml"],
        ddl_path=None,
        dml_path=None,
        messages=["Validation failed: bad sql"],
    )
    calls: list[str] = []

    def fake_migration(*, table_name: str, **_kwargs):
        calls.append(table_name)
        return f"```sql\nCREATE TABLE IF NOT EXISTS {table_name} (id INT);\n```"

    with (
        patch("spark_to_flink.cli.llm_reachable", return_value=True),
        patch("spark_to_flink.cli.resolve_llm_model", return_value="test-model"),
        patch("spark_to_flink.cli.run_migration", side_effect=fake_migration),
        patch("spark_to_flink.cli.clean_flink_sql_and_validate", return_value=failure),
    ):
        first = runner.invoke(
            app,
            ["--file", str(spark_file), "--out-dir", str(out_dir), "--skip-deploy"],
        )
    assert first.exit_code == 1
    assert calls == ["clicks"]
    calls.clear()
    with (
        patch("spark_to_flink.cli.llm_reachable", return_value=True),
        patch("spark_to_flink.cli.resolve_llm_model", return_value="test-model"),
        patch("spark_to_flink.cli.run_migration", side_effect=fake_migration),
        patch("spark_to_flink.cli.clean_flink_sql_and_validate", return_value=None),
    ):
        second = runner.invoke(
            app,
            ["--file", str(spark_file), "--out-dir", str(out_dir), "--skip-deploy"],
        )
    assert second.exit_code == 0, second.output
    assert calls == ["clicks", "detected"]
    manifest = load_manifest(statements_dir_for(spark_file))
    assert manifest is not None
    assert all(e.status == "migrated" for e in manifest.statements)
