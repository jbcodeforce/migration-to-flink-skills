"""Unit tests for Flink SQL validation."""

from unittest.mock import patch

import pytest

from flink_skill_common.deploy.flink_statement_manager import (
    FlinkStatementManager,
    StatementManagerError,
)
from flink_skill_common.sql_validate import (
    SqlValidationError,
    raise_on_errors,
    validate_statements,
    validate_statements_remote,
)

VALID_DDL = """CREATE TABLE IF NOT EXISTS publication_events (
    bookid BIGINT,
    author STRING,
    title STRING
) DISTRIBUTED BY HASH(bookid) INTO 1 BUCKETS
WITH (
    'connector' = 'kafka',
    'topic' = 'publication_events'
);"""

VALID_DML = """INSERT INTO george_martin_books
SELECT bookid, author, title
FROM publication_events
WHERE author = 'George R. R. Martin';"""


def test_validate_statements_accepts_valid_fixture():
    issues = validate_statements([VALID_DDL], [VALID_DML])
    errors = [i for i in issues if i.severity == "error"]
    assert not errors


def test_validate_statements_empty_lists():
    assert validate_statements([], []) == []


def test_validate_statements_missing_paren():
    issues = validate_statements(["CREATE TABLE t (id STRING"], [])
    errors = [i for i in issues if i.severity == "error"]
    assert errors
    assert errors[0].kind == "ddl"


def test_validate_statements_insert_typo():
    issues = validate_statements([], ["INSRT INTO t SELECT 1"])
    errors = [i for i in issues if i.severity == "error"]
    assert errors
    assert errors[0].kind == "dml"
    assert "INSERT INTO" in errors[0].message


def test_validate_statements_select_typo_in_dml():
    issues = validate_statements([], ["INSERT INTO t SELECT * FORM src"])
    errors = [i for i in issues if i.severity == "error"]
    assert errors


def test_raise_on_errors_raises():
    issues = validate_statements(["CREATE TABLE t (id STRING"], [])
    with pytest.raises(SqlValidationError):
        raise_on_errors(issues)


def test_validate_statements_remote_delegates(settings):
    manager = FlinkStatementManager(settings)
    with patch(
        "flink_skill_common.deploy.flink_statement_manager.FlinkStatementManager",
        return_value=manager,
    ):
        with patch.object(manager, "validate_statements", return_value=[]) as mock_validate:
            with patch("flink_skill_common.config.require_flink_deploy_ready"):
                issues = validate_statements_remote(["CREATE TABLE t (id STRING);"], [])
    assert issues == []
    mock_validate.assert_called_once_with(["CREATE TABLE t (id STRING);"], [])


def test_flink_statement_manager_validate_sql_success(settings):
    manager = FlinkStatementManager(settings)

    with patch.object(
        manager,
        "create_statement",
        return_value={"phase": "COMPLETED", "detail": "ok"},
    ):
        with patch.object(manager, "delete_statement", return_value={"status": "deleted"}):
            issues = manager.validate_sql("CREATE TABLE t (id STRING);", kind="ddl", index=0)

    assert issues == []


def test_flink_statement_manager_validate_sql_submit_error(settings):
    manager = FlinkStatementManager(settings)

    with patch.object(
        manager,
        "create_statement",
        side_effect=StatementManagerError("syntax error near 'FOO'"),
    ):
        with patch.object(manager, "delete_statement"):
            issues = manager.validate_sql("CREATE TABLE t (id FOO);", kind="ddl", index=0)

    assert len(issues) == 1
    assert issues[0].severity == "error"
    assert "syntax error" in issues[0].message


@pytest.fixture
def settings():
    from flink_skill_common.config import FlinkDeploySettings

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
