"""Offline CLI tests for migrate command deploy wiring."""

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from ksql_to_flink.cli import app
from flink_skill_common.deploy.flink_statement_manager import DeployResult
from ksql_ref_fixtures import ksql_source_path, KSQL_MIGRATE_CASES

runner = CliRunner()

def test_validate_configuration():
    case = KSQL_MIGRATE_CASES[0]
    ksqkl_to_migrate = ksql_source_path(case)
    assert ksqkl_to_migrate.exists()


def test_migrate_deploys_by_default(tmp_path: Path):
    case = KSQL_MIGRATE_CASES[0]
    ksqkl_to_migrate = ksql_source_path(case)
    out_dir = tmp_path / "out"
    deploy_result = DeployResult(
        table_name=case.target_table,
        ddl_statement="my-table-ddl",
        dml_statement="",
        ddl_phase="COMPLETED",
        dml_phase="",
        success=True,
        messages=["ok"],
    )

    


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
