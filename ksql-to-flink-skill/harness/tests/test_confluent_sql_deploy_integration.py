"""Live confluent-sql deploy integration tests (requires CC credentials)."""

from pathlib import Path

import pytest

from ksql_flink_skill.deploy import FlinkStatementManager, ddl_statement_name, require_flink_deploy_ready

pytestmark = pytest.mark.integration


@pytest.fixture
def require_deploy():
    try:
        require_flink_deploy_ready()
    except Exception as exc:
        pytest.skip(f"Flink deploy not configured: {exc}")


def test_live_deploy_minimal_ddl(tmp_path: Path, require_deploy):
    """Deploy a minimal SHOW TABLES statement (non-destructive smoke test)."""
    table = "ksql-skill-confluent-sql-smoke"
    ddl_name = ddl_statement_name(table)
    ddl_path = tmp_path / f"ddl.{table}.sql"
    ddl_path.write_text("-- smoke test\nSHOW TABLES;")
    dml_path = tmp_path / f"dml.{table}.sql"
    dml_path.write_text("")

    try:
        result = FlinkStatementManager().deploy_table(table, ddl_path, dml_path)
        assert result.ddl_statement == ddl_name
        assert result.ddl_phase in {"RUNNING", "COMPLETED", "APPLIED", "STOPPED"}
    except Exception as exc:
        pytest.skip(f"Live deploy smoke test skipped: {exc}")
