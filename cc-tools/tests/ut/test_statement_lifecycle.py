"""Unit tests for cc_deploy.statement_lifecycle."""

from unittest.mock import MagicMock

import pytest
from confluent_sql.exceptions import OperationalError, StatementNotFoundError

from cc_deploy.statement_lifecycle import (
    StatementLifecycleError,
    check_statement_health,
    classify_sql,
    create_statement,
    delete_statement,
    get_statement_exceptions,
    statement_status,
    submit_statement,
    wait_for_phase,
)


@pytest.fixture
def config() -> dict[str, str]:
    return {
        "FLINK_API_KEY": "key",
        "FLINK_API_SECRET": "secret",
        "ORGANIZATION_ID": "org-1",
        "ENVIRONMENT_ID": "env-1",
        "FLINK_COMPUTE_POOL_ID": "pool-1",
        "FLINK_DATABASE_NAME": "db-1",
        "CLOUD_PROVIDER": "aws",
        "CLOUD_REGION": "us-west-2",
    }


def test_classify_sql():
    assert classify_sql("CREATE TABLE t (id STRING);") == "snapshot_ddl"
    assert classify_sql("INSERT INTO t SELECT id FROM src;") == "streaming_dml"
    assert classify_sql("INSERT INTO t VALUES (1);") == "batch_dml"
    assert classify_sql("CREATE TABLE t AS SELECT id FROM src;") == "streaming_ddl"
    assert classify_sql("DROP TABLE IF EXISTS t;") == "snapshot_ddl"


def test_statement_status_not_found():
    conn = MagicMock()
    conn.get_statement.side_effect = StatementNotFoundError("gone", "t-ddl")
    status = statement_status(conn, "t-ddl")
    assert status["phase"] == "NOT_FOUND"


def test_submit_snapshot_ddl(config):
    conn = MagicMock()
    stmt = MagicMock()
    stmt.status = {"phase": "COMPLETED", "detail": "ok"}
    conn.execute_snapshot_ddl.return_value = stmt

    result = submit_statement(conn, config, "t-ddl", "CREATE TABLE t (id STRING);")
    assert result["phase"] == "COMPLETED"
    assert result["kind"] == "snapshot_ddl"
    conn.execute_snapshot_ddl.assert_called_once()
    props = conn.execute_snapshot_ddl.call_args.kwargs["properties"]
    assert "sql.dry-run" not in props


def test_submit_dry_run_sets_property(config):
    conn = MagicMock()
    stmt = MagicMock()
    stmt.status = {"phase": "COMPLETED", "detail": ""}
    conn.execute_snapshot_ddl.return_value = stmt

    submit_statement(
        conn, config, "t-ddl", "CREATE TABLE t (id STRING);", dry_run=True
    )
    props = conn.execute_snapshot_ddl.call_args.kwargs["properties"]
    assert props["sql.dry-run"] == "true"


def test_create_statement_retries_on_409(config):
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

    sleeps: list[float] = []
    result = create_statement(
        conn,
        config,
        "t-ddl",
        "CREATE TABLE t (id STRING);",
        timeout=1.0,
        poll=0.01,
        sleep=sleeps.append,
    )

    assert result["phase"] == "RUNNING"
    conn.delete_statement.assert_called_once_with("t-ddl")


def test_wait_for_phase_success():
    conn = MagicMock()
    pending = MagicMock()
    pending.status = {"phase": "PENDING", "detail": ""}
    running = MagicMock()
    running.status = {"phase": "RUNNING", "detail": ""}
    conn.get_statement.side_effect = [pending, running]

    result = wait_for_phase(
        conn, "t-ddl", {"RUNNING"}, timeout=1.0, poll=0.01, sleep=lambda _: None
    )
    assert result["phase"] == "RUNNING"


def test_wait_for_phase_timeout():
    conn = MagicMock()
    pending = MagicMock()
    pending.status = {"phase": "PENDING", "detail": ""}
    conn.get_statement.return_value = pending

    with pytest.raises(StatementLifecycleError, match="Timeout"):
        wait_for_phase(
            conn, "t-ddl", {"RUNNING"}, timeout=0.05, poll=0.01, sleep=lambda _: None
        )


def test_wait_for_phase_returns_failure_as_terminal():
    conn = MagicMock()
    failed = MagicMock()
    failed.status = {"phase": "FAILED", "detail": "boom"}
    conn.get_statement.return_value = failed

    result = wait_for_phase(
        conn, "t-ddl", {"RUNNING"}, timeout=1.0, poll=0.01, sleep=lambda _: None
    )
    assert result["phase"] == "FAILED"


def test_delete_statement_not_found():
    conn = MagicMock()
    conn.delete_statement.side_effect = StatementNotFoundError("gone", "t-ddl")
    result = delete_statement(conn, "t-ddl")
    assert result["status"] == "not_found"


def test_get_statement_exceptions():
    conn = MagicMock()
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"exceptions": [{"message": "boom"}]}
    conn._request.return_value = resp

    result = get_statement_exceptions(conn, "t-dml")
    assert result["exceptions"][0]["message"] == "boom"


def test_check_statement_health():
    conn = MagicMock()
    stmt = MagicMock()
    stmt.status = {"phase": "RUNNING", "detail": ""}
    conn.get_statement.return_value = stmt

    result = check_statement_health(conn, "t-dml")
    assert result["healthy"] is True
    assert result["phase"] == "RUNNING"
