"""Offline CLI tests for migrate command progress output."""

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from flink_skill_common.config import HarnessContext, configure
from flink_skill_common.convergence import ConvergenceResult

_HARNESS_ROOT = Path(__file__).resolve().parents[2]
_PROJECT_ROOT = _HARNESS_ROOT.parent.parent

configure(HarnessContext(harness_root=_HARNESS_ROOT, project_root=_PROJECT_ROOT))

from ksql_to_flink.cli import app

runner = CliRunner()

VALID_DDL = """CREATE TABLE IF NOT EXISTS george_martin (
    id INT
) DISTRIBUTED BY HASH(id) INTO 1 BUCKETS
WITH ('changelog.mode' = 'append');"""

VALID_DML = "INSERT INTO george_martin SELECT id FROM src;"


def test_migrate_verbose_progress_with_mocks(tmp_path: Path):
    ksql_file = tmp_path / "input.ksql"
    ksql_file.write_text(
        "CREATE STREAM SONGS (id INT) WITH (kafka_topic='songs', value_format='JSON');"
    )
    out_dir = tmp_path / "output"
    table_dir = out_dir / "george_martin"
    agent_response = f"DDL\n```sql\n{VALID_DDL}\n```\nDML\n```sql\n{VALID_DML}\n```"
    convergence_result = ConvergenceResult(
        success=True,
        ddls=[VALID_DDL],
        dmls=[VALID_DML],
        ddl_path=table_dir / "ddl.george_martin.sql",
        dml_path=table_dir / "dml.george_martin.sql",
        messages=["Offline validation passed.", "Skipped deploy (--skip-deploy)."],
    )

    def _fake_converge(*args, **kwargs):
        on_progress = kwargs.get("on_progress")
        if on_progress:
            on_progress("Running offline validation...")
            on_progress("Offline validation passed.")
        return convergence_result

    with (
        patch("ksql_to_flink.cli.llm_reachable", return_value=True),
        patch("ksql_to_flink.cli.resolve_llm_model", return_value="test-model"),
        patch("ksql_to_flink.cli.run_migration", return_value=agent_response),
        patch(
            "flink_skill_common.sql_validate.compute_missing_source_tables",
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
                "george_martin",
                "--file",
                str(ksql_file),
                "--out-dir",
                str(out_dir),
                "--skip-deploy",
            ],
        )

    assert result.exit_code == 0
    output = result.output
    assert "ksql-flink-migrate" in output
    assert "test-model" in output
    assert "Found 1 CREATE statement(s)" in output
    assert "Running translation agent" in output
    assert "Extracted 1 DDL, 1 DML" in output
    assert "Running offline validation" in output
    assert "Offline validation passed" in output
    assert mock_converge.call_args.kwargs["on_progress"] is not None
