"""Tests for FlinkStatementManager migration adapter over cc_deploy."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from confluent_sql.exceptions import OperationalError, StatementNotFoundError

from flink_skill_common.config import FlinkDeploySettings
from flink_skill_common.deploy.flink_statement_manager import (
    FlinkStatementManager,
    StatementManagerError,
    classify_sql,
    ddl_statement_name,
    dml_statement_name,
    settings_to_cc_config,
)
from flink_skill_common.sql_parse import extract_statement_table_name


@pytest.fixture
def settings() -> FlinkDeploySettings:
    return FlinkDeploySettings(
        flink_api_key="key",
        flink_api_secret="secret",
        organization_id="org-1",
        environment_id="env-1",
        compute_pool_id="pool-1",
        database_name="db-1",
        endpoint="https://flink.example.com",
        cloud_provider="aws",
        cloud_region="us-west-2",
        poll_seconds=0.01,
        timeout_seconds=1.0,
    )


def test_settings_to_cc_config(settings):
    cfg = settings_to_cc_config(settings)
    assert cfg["FLINK_API_KEY"] == "key"
    assert cfg["ENVIRONMENT_ID"] == "env-1"
    assert cfg["FLINK_REST_ENDPOINT"] == "https://flink.example.com"


def test_classify_sql_delegates():
    assert classify_sql("CREATE TABLE t (id STRING);") == "snapshot_ddl"
    assert classify_sql("INSERT INTO t SELECT id FROM src;") == "streaming_dml"
    assert classify_sql("CREATE TABLE t AS SELECT id FROM src;") == "streaming_ddl"


def test_extract_table_name():
    assert extract_statement_table_name("CREATE TABLE t (id STRING);") == "t"
    assert extract_statement_table_name("INSERT INTO t \nSELECT id FROM src;") == "t"
    assert extract_statement_table_name("CREATE TABLE IF NOT EXISTS t \n(id STRING);") == "t"
    assert extract_statement_table_name(
        "CREATE TABLE IF NOT EXISTS t \n(id STRING) WITH (kafka.topic = 't');"
    ) == "t"


def test_create_statement_snapshot_ddl(settings):
    manager = FlinkStatementManager(settings)
    conn = MagicMock()
    stmt = MagicMock()
    stmt.status = {"phase": "COMPLETED", "detail": "ok"}
    conn.execute_snapshot_ddl.return_value = stmt

    with patch.object(manager, "connect") as mock_connect:
        mock_connect.return_value.__enter__.return_value = conn
        result = manager.create_statement("t-ddl", "CREATE TABLE t (id STRING);")

    assert result["phase"] == "COMPLETED"
    conn.execute_snapshot_ddl.assert_called_once()
    props = conn.execute_snapshot_ddl.call_args.kwargs["properties"]
    assert "sql.dry-run" not in props


def test_create_statement_dry_run(settings):
    manager = FlinkStatementManager(settings)
    conn = MagicMock()
    stmt = MagicMock()
    stmt.status = {"phase": "COMPLETED", "detail": "ok"}
    conn.execute_snapshot_ddl.return_value = stmt

    with patch.object(manager, "connect") as mock_connect:
        mock_connect.return_value.__enter__.return_value = conn
        manager.create_statement("t-ddl", "CREATE TABLE t (id STRING);", dry_run=True)

    props = conn.execute_snapshot_ddl.call_args.kwargs["properties"]
    assert props["sql.dry-run"] == "true"


def test_create_statement_retries_on_409(settings):
    manager = FlinkStatementManager(settings)
    conn = MagicMock()
    stmt = MagicMock()
    stmt.status = {"phase": "RUNNING", "detail": ""}

    conn.execute_snapshot_ddl.side_effect = [
        OperationalError("exists", http_status_code=409),
        stmt,
    ]
    conn.get_statement.side_effect = [
        MagicMock(),
        StatementNotFoundError("gone", "t-ddl"),
    ]

    with patch.object(manager, "connect") as mock_connect:
        mock_connect.return_value.__enter__.return_value = conn
        result = manager.create_statement("t-ddl", "CREATE TABLE t (id STRING);")

    assert result["phase"] == "RUNNING"
    conn.delete_statement.assert_called_once_with("t-ddl")


def test_wait_for_phase_success(settings):
    manager = FlinkStatementManager(settings)
    conn = MagicMock()
    pending = MagicMock()
    pending.status = {"phase": "PENDING", "detail": ""}
    running = MagicMock()
    running.status = {"phase": "RUNNING", "detail": ""}
    conn.get_statement.side_effect = [pending, running]

    with patch.object(manager, "connect") as mock_connect:
        mock_connect.return_value.__enter__.return_value = conn
        result = manager.wait_for_phase("t-ddl", {"RUNNING"})
    assert result["phase"] == "RUNNING"


def test_wait_for_phase_timeout(settings):
    manager = FlinkStatementManager(settings)
    conn = MagicMock()
    pending = MagicMock()
    pending.status = {"phase": "PENDING", "detail": ""}
    conn.get_statement.return_value = pending

    with patch.object(manager, "connect") as mock_connect:
        mock_connect.return_value.__enter__.return_value = conn
        with pytest.raises(StatementManagerError, match="Timeout"):
            manager.wait_for_phase("t-ddl", {"RUNNING"}, timeout=0.05)


def test_wait_for_phase_propagates_keyboard_interrupt(settings):
    manager = FlinkStatementManager(settings)
    conn = MagicMock()
    pending = MagicMock()
    pending.status = {"phase": "PENDING", "detail": ""}
    conn.get_statement.return_value = pending

    with patch.object(manager, "connect") as mock_connect:
        mock_connect.return_value.__enter__.return_value = conn
        with patch(
            "flink_skill_common.deploy.flink_statement_manager.interruptible_sleep",
            side_effect=KeyboardInterrupt,
        ):
            with pytest.raises(KeyboardInterrupt):
                manager.wait_for_phase("t-ddl", {"RUNNING"}, timeout=1.0)


def test_get_statement_exceptions(settings):
    manager = FlinkStatementManager(settings)
    conn = MagicMock()
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"exceptions": [{"message": "boom"}]}
    conn._request.return_value = resp

    with patch.object(manager, "connect") as mock_connect:
        mock_connect.return_value.__enter__.return_value = conn
        result = manager.get_statement_exceptions("t-dml")

    assert result["exceptions"][0]["message"] == "boom"


def test_deploy_table_deletes_ddl_statement_after_success(settings, tmp_path: Path):
    manager = FlinkStatementManager(settings)
    ddl_path = tmp_path / "ddl.my_table.sql"
    dml_path = tmp_path / "dml.my_table.sql"
    ddl_path.write_text("CREATE TABLE my_table (id INT);")
    dml_path.write_text("INSERT INTO my_table SELECT id FROM src;")

    with patch.object(manager, "create_statement", return_value={"phase": "COMPLETED"}):
        with patch.object(manager, "_wait_for_deploy_phase", return_value="COMPLETED"):
            with patch.object(manager, "check_statement_health", return_value={"healthy": True}):
                with patch.object(manager, "_delete_statement_safe") as mock_delete:
                    manager.deploy_table("my_table", ddl_path, dml_path)

    mock_delete.assert_called_once_with(ddl_statement_name("my_table"))


def test_cleanup_deployed_table_deletes_dml_and_drops_tables(settings, tmp_path: Path):
    manager = FlinkStatementManager(settings)
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    (tests_dir / "ddl.src.sql").write_text("CREATE TABLE src (id INT);")

    with patch.object(manager, "_delete_statement_safe") as mock_delete:
        with patch.object(manager, "drop_table") as mock_drop:
            manager.cleanup_deployed_table("my_table", tests_dir)

    mock_delete.assert_called_once_with(dml_statement_name("my_table"))
    mock_drop.assert_any_call("my_table")
    mock_drop.assert_any_call("src")
    assert mock_drop.call_count == 2
