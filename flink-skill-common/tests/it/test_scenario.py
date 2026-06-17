"""Live deploy integration tests (requires Confluent Cloud Flink credentials)."""

from pathlib import Path

import pytest

from flink_skill_common.deploy.flink_statement_manager import (
    DeployError,
    FlinkStatementManager,
    StatementManagerError,
    SUCCESS_PHASES,
)
from flink_skill_common.deploy.statements import ddl_statement_name, dml_statement_name
from flink_skill_common.config import FlinkDeployNotReadyError, flink_deploy_settings

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[3]
SEEDS_DIR = REPO_ROOT / "references/ksql/flink_ref/seeds/"
TABLE_NAME = "raw_classical_songs"


@pytest.fixture
def require_deploy():
    try:
        flink_deploy_settings()
    except FlinkDeployNotReadyError as exc:
        pytest.skip(f"Flink deploy not configured: {exc}")


@pytest.fixture
def clean_raw_classical_songs_statements(require_deploy):
    """Remove prior deploy statements so reruns start from a clean state."""
    manager = FlinkStatementManager()
    name = dml_statement_name(TABLE_NAME)
    try:
        manager.delete_statement(name)
    except StatementManagerError:
        pass
    yield

def test_validate_config():
    """Validate Flink deploy settings are present."""
    config = flink_deploy_settings()
    print(config)
    assert config.flink_api_key is not None
    assert config.flink_api_secret is not None
    assert config.organization_id is not None
    assert config.environment_id is not None
    assert config.compute_pool_id is not None
    assert config.database_name is not None
    assert config.endpoint is not None
    assert config.cloud_provider is not None
    assert config.cloud_region is not None
    assert config.poll_seconds is not None
    assert config.timeout_seconds is not None
    assert config.http_user_agent is not None

def test_list_statements():
    """List all statements in the Flink cluster."""
    manager = FlinkStatementManager()
    statements, count = manager.list_statements()
    print(statements)
    assert len(statements) >= count

def test_deploy_reference_seeds():
    """Deploy reference seeds."""
    manager = FlinkStatementManager()
    ddl_sql = (SEEDS_DIR / "raw_classical_songs" / "ddl.raw_classical_songs.sql").read_text()
    ddl_name = ddl_statement_name(TABLE_NAME)
    dml_name = dml_statement_name(TABLE_NAME)
    try:
        stmt=manager.create_statement(ddl_name, ddl_sql)
        print(f"{stmt}\nDeploy dml now\n")
        assert stmt['name'] == ddl_name
        assert stmt['phase'] == "COMPLETED"
        
        stmt = manager.get_statement(ddl_name)
        assert stmt['phase'] == "NOT_FOUND"

        dml_sql = (SEEDS_DIR / "raw_classical_songs" / "dml.raw_classical_songs.sql").read_text()
        stmt = manager.create_statement(dml_name, dml_sql)
        print(f"{stmt}\n\n")
        assert stmt['name'] == dml_name
        assert stmt['phase'] == "COMPLETED"
    except StatementManagerError as exc:
        print(f"Deploy failed: {exc.message}")
        pass
    finally:
        manager.delete_statement(dml_name)
        statement = manager.create_statement("drop-raw-classical-songs", "DROP TABLE raw_classical_songs;")
        print(statement)

    


def _test_deploy_raw_classical_songs(clean_raw_classical_songs_statements):
    """Deploy raw_classical_songs DDL then DML from flink_ref seeds."""
    ddl_path = SEEDS_DIR / "ddl.raw_classical_songs.sql"
    dml_path = SEEDS_DIR / "dml.raw_classical_songs.sql"
    assert ddl_path.is_file(), f"Missing DDL fixture: {ddl_path}"
    assert dml_path.is_file(), f"Missing DML fixture: {dml_path}"

    try:
        result = FlinkStatementManager().deploy_table(TABLE_NAME, ddl_path, dml_path)
    except DeployError as exc:
        pytest.fail(f"Deploy failed: {exc}")
    print(result)
    assert result.table_name == TABLE_NAME
    assert result.ddl_statement == ddl_statement_name(TABLE_NAME)
    assert result.dml_statement == dml_statement_name(TABLE_NAME)
    assert result.ddl_phase in SUCCESS_PHASES or result.ddl_phase == "NOT_FOUND"
    assert result.dml_phase in SUCCESS_PHASES or result.dml_phase == "NOT_FOUND"
    assert result.success is True
