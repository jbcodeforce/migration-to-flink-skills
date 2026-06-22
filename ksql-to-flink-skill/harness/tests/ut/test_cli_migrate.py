"""Offline CLI tests for migrate command deploy wiring."""

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from ksql_to_flink.cli import app
from flink_skill_common.deploy.flink_statement_manager import DeployResult


runner = CliRunner()


def test_migrate_skip_deploy(tmp_path: Path):
    ksql_file = tmp_path / "test.ksql"
    ksql_file.write_text("CREATE STREAM s (id INT) WITH (KAFKA_TOPIC='t');")
    out_dir = tmp_path / "out"

    with patch("ksql_to_flink.cli.llm_reachable", return_value=True):
        with patch("ksql_to_flink.cli.run_migration", return_value="```sql\nCREATE TABLE t (id INT);\n```"):
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
    assert result.exit_code == 0
    assert "Skipped deploy" in result.output
    assert "Found 1 CREATE" in result.output



def test_migrate_deploys_by_default(tmp_path: Path):
    ksql_file = tmp_path / "test.ksql"
    ksql_file.write_text("CREATE STREAM s (id INT) WITH (KAFKA_TOPIC='t');")
    out_dir = tmp_path / "out"
    deploy_result = DeployResult(
        table_name="my_table",
        ddl_statement="my-table-ddl",
        dml_statement="",
        ddl_phase="COMPLETED",
        dml_phase="",
        success=True,
        messages=["ok"],
    )

    with patch("ksql_to_flink.cli.llm_reachable", return_value=True):
        with patch(
            "ksql_to_flink.cli.run_migration",
            return_value=(
                "DDL\n```sql\nCREATE TABLE IF NOT EXISTS my_table (id INT);\n```\n"
                "DML\n```sql\n\n```"
            ),
        ):

            with patch("ksql_to_flink.pipeline.validate_statements_remote", return_value=[]):
                with patch("ksql_to_flink.pipeline.FlinkStatementManager") as mock_manager_cls:
                    mock_manager_cls.return_value.deploy_table.return_value = deploy_result
                    result = runner.invoke(
                        app,
                        [
                            "--table",
                            "my_table",
                            "--file",
                            str(ksql_file),
                            "--out-dir",
                            str(out_dir),
                        ],
                    )
    assert result.exit_code == 0
    assert "Deploy OK" in result.output
    mock_manager_cls.return_value.deploy_table.assert_called_once()
