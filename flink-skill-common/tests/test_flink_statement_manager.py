"""Tests for FlinkStatementManager."""

import json
from unittest.mock import MagicMock, patch

import pytest
from confluent_sql.exceptions import OperationalError, StatementNotFoundError

from flink_skill_common.config import FlinkDeploySettings
from flink_skill_common.deploy.flink_statement_manager import (
    FlinkStatementManager,
    StatementManagerError,
    classify_sql,
)


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


def test_classify_sql():
    assert classify_sql("CREATE TABLE t (id STRING);") == "snapshot_ddl"
    assert classify_sql("INSERT INTO t SELECT id FROM src;") == "streaming_dml"


def test_create_statement_snapshot_ddl(settings):
    manager = FlinkStatementManager(settings)
    conn = MagicMock()
    stmt = MagicMock()
    stmt.phase.name = "COMPLETED"
    stmt.status = {"detail": "ok"}
    conn.execute_snapshot_ddl.return_value = stmt

    with patch.object(manager, "connect") as mock_connect:
        mock_connect.return_value.__enter__.return_value = conn
        result = manager.create_statement("t-ddl", "CREATE TABLE t (id STRING);")

    assert result["phase"] == "COMPLETED"
    conn.execute_snapshot_ddl.assert_called_once()


def test_create_statement_retries_on_409(settings):
    manager = FlinkStatementManager(settings)
    conn = MagicMock()
    stmt = MagicMock()
    stmt.phase.name = "RUNNING"
    stmt.status = {}

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
    with patch.object(
        manager,
        "get_statement",
        side_effect=[
            {"name": "t-ddl", "phase": "PENDING", "detail": ""},
            {"name": "t-ddl", "phase": "RUNNING", "detail": ""},
        ],
    ):
        result = manager.wait_for_phase("t-ddl", {"RUNNING"})
    assert result["phase"] == "RUNNING"


def test_wait_for_phase_timeout(settings):
    manager = FlinkStatementManager(settings)
    with patch.object(
        manager,
        "get_statement",
        return_value={"name": "t-ddl", "phase": "PENDING", "detail": ""},
    ):
        with pytest.raises(StatementManagerError, match="Timeout"):
            manager.wait_for_phase("t-ddl", {"RUNNING"}, timeout=0.05)


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


def test_list_statements(settings):
    manager = FlinkStatementManager(settings)
    conn = MagicMock()
    resp = MagicMock()
    resp.json.return_value = {
        "data": [{"name": "t-ddl", "status": {"phase": "RUNNING", "detail": ""}}]
    }
    conn._request.return_value = resp

    with patch.object(manager, "connect") as mock_connect:
        mock_connect.return_value.__enter__.return_value = conn
        result = manager.list_statements(page_size=10)

    assert result["count"] == 1
    assert result["statements"][0]["name"] == "t-ddl"
