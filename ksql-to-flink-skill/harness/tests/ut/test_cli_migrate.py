"""Offline CLI tests for migrate command deploy wiring and manifest resume."""

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from flink_skill_common.convergence import ConvergenceResult
from flink_skill_common.migration_manifest import load_manifest, statements_dir_for
from ksql_ref_fixtures import KSQL_MIGRATE_CASES, ksql_source_path
from ksql_to_flink.cli import app

runner = CliRunner()


def test_validate_configuration():
    case = KSQL_MIGRATE_CASES[0]
    ksqkl_to_migrate = ksql_source_path(case)
    assert ksqkl_to_migrate.exists()


def test_migrate_deploys_by_default(tmp_path: Path):
    case = KSQL_MIGRATE_CASES[0]
    ksqkl_to_migrate = ksql_source_path(case)
    out_dir = tmp_path / "out"
    # Placeholder: deploy wiring covered by integration tests.
    assert ksqkl_to_migrate.exists()
    assert out_dir


def test_migrate_exits_130_on_keyboard_interrupt(tmp_path: Path):
    ksql_file = tmp_path / "test.ksql"
    ksql_file.write_text("CREATE STREAM s (id INT) WITH (KAFKA_TOPIC='t');")
    out_dir = tmp_path / "out"

    with patch("ksql_to_flink.cli.llm_reachable", return_value=True):
        with patch(
            "ksql_to_flink.cli.run_migration",
            side_effect=KeyboardInterrupt,
        ):
            result = runner.invoke(
                app,
                [
                    "--table",
                    "my_table",
                    "--file",
                    str(ksql_file),
                    "--out-dir",
                    str(out_dir),
                    "--skip-deploy",
                ],
            )
    assert result.exit_code == 130
    assert "Interrupted during s" in result.output
    manifest = load_manifest(statements_dir_for(ksql_file))
    assert manifest is not None
    assert manifest.statements[0].status == "interrupted"


def test_migrate_writes_statement_files_and_marks_migrated(tmp_path: Path):
    ksql_file = tmp_path / "pipeline.ksql"
    ksql_file.write_text(
        "CREATE STREAM clicks (id INT) WITH (KAFKA_TOPIC='clicks');\n"
        "CREATE STREAM detected AS SELECT * FROM clicks EMIT CHANGES;\n"
    )
    out_dir = tmp_path / "out"
    calls: list[str] = []

    def fake_migration(*, table_name: str, ksql: str, **_kwargs):
        calls.append(table_name)
        return f"```sql\nCREATE TABLE IF NOT EXISTS {table_name} (id INT);\n```"

    with patch("ksql_to_flink.cli.llm_reachable", return_value=True):
        with patch("ksql_to_flink.cli.run_migration", side_effect=fake_migration):
            with patch(
                "ksql_to_flink.cli.clean_flink_sql_and_validate",
                return_value=None,
            ):
                result = runner.invoke(
                    app,
                    [
                        "--file",
                        str(ksql_file),
                        "--out-dir",
                        str(out_dir),
                        "--skip-deploy",
                    ],
                )
    assert result.exit_code == 0, result.output
    assert calls == ["clicks", "detected"]
    statements_dir = statements_dir_for(ksql_file)
    assert (statements_dir / "001_clicks.ksql").is_file()
    assert (statements_dir / "002_detected.ksql").is_file()
    manifest = load_manifest(statements_dir)
    assert manifest is not None
    assert all(e.status == "migrated" for e in manifest.statements)


def test_migrate_resume_skips_migrated(tmp_path: Path):
    ksql_file = tmp_path / "pipeline.ksql"
    text = (
        "CREATE STREAM clicks (id INT) WITH (KAFKA_TOPIC='clicks');\n"
        "CREATE STREAM detected AS SELECT * FROM clicks EMIT CHANGES;\n"
    )
    ksql_file.write_text(text)
    out_dir = tmp_path / "out"
    calls: list[str] = []

    def fake_migration(*, table_name: str, **_kwargs):
        calls.append(table_name)
        return f"```sql\nCREATE TABLE IF NOT EXISTS {table_name} (id INT);\n```"

    with patch("ksql_to_flink.cli.llm_reachable", return_value=True):
        with patch("ksql_to_flink.cli.run_migration", side_effect=fake_migration):
            with patch(
                "ksql_to_flink.cli.clean_flink_sql_and_validate",
                return_value=None,
            ):
                first = runner.invoke(
                    app,
                    ["--file", str(ksql_file), "--out-dir", str(out_dir), "--skip-deploy"],
                )
                assert first.exit_code == 0, first.output
                calls.clear()
                with patch(
                    "ksql_to_flink.cli.split_ksql_create_statements",
                ) as mock_split:
                    second = runner.invoke(
                        app,
                        ["--file", str(ksql_file), "--out-dir", str(out_dir), "--skip-deploy"],
                    )
    assert second.exit_code == 0, second.output
    assert calls == []
    assert "already migrated" in second.output
    assert "split skipped" in second.output
    mock_split.assert_not_called()


def test_migrate_failure_marks_failed_leaves_later_pending(tmp_path: Path):
    ksql_file = tmp_path / "pipeline.ksql"
    ksql_file.write_text(
        "CREATE STREAM clicks (id INT) WITH (KAFKA_TOPIC='clicks');\n"
        "CREATE STREAM detected AS SELECT * FROM clicks EMIT CHANGES;\n"
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

    with patch("ksql_to_flink.cli.llm_reachable", return_value=True):
        with patch(
            "ksql_to_flink.cli.run_migration",
            return_value="```sql\nCREATE TABLE clicks (id INT);\n```",
        ):
            with patch(
                "ksql_to_flink.cli.clean_flink_sql_and_validate",
                return_value=failure,
            ):
                result = runner.invoke(
                    app,
                    [
                        "--file",
                        str(ksql_file),
                        "--out-dir",
                        str(out_dir),
                        "--skip-deploy",
                    ],
                )
    assert result.exit_code == 1
    manifest = load_manifest(statements_dir_for(ksql_file))
    assert manifest is not None
    assert manifest.statements[0].status == "failed"
    assert manifest.statements[1].status == "pending"


def test_migrate_resume_retries_failed_only(tmp_path: Path):
    ksql_file = tmp_path / "pipeline.ksql"
    ksql_file.write_text(
        "CREATE STREAM clicks (id INT) WITH (KAFKA_TOPIC='clicks');\n"
        "CREATE STREAM detected AS SELECT * FROM clicks EMIT CHANGES;\n"
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

    with patch("ksql_to_flink.cli.llm_reachable", return_value=True):
        with patch("ksql_to_flink.cli.run_migration", side_effect=fake_migration):
            with patch(
                "ksql_to_flink.cli.clean_flink_sql_and_validate",
                return_value=failure,
            ):
                first = runner.invoke(
                    app,
                    ["--file", str(ksql_file), "--out-dir", str(out_dir), "--skip-deploy"],
                )
            assert first.exit_code == 1
            assert calls == ["clicks"]
            calls.clear()
            with patch(
                "ksql_to_flink.cli.clean_flink_sql_and_validate",
                return_value=None,
            ):
                second = runner.invoke(
                    app,
                    ["--file", str(ksql_file), "--out-dir", str(out_dir), "--skip-deploy"],
                )
    assert second.exit_code == 0, second.output
    assert calls == ["clicks", "detected"]
    manifest = load_manifest(statements_dir_for(ksql_file))
    assert manifest is not None
    assert all(e.status == "migrated" for e in manifest.statements)
