"""Unit tests for Flink SQL validation."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlglot.errors import ParseError

from flink_ref_fixtures import load_all_valid_flink_reference_sql
from flink_skill_common.config import HarnessContext, configure
from flink_skill_common.sql_validate import (
    SqlValidationError,
    SqlValidationIssue,
    _extract_ddl_header,
    _parse_error_message,
    _validate_one,
    _validate_parseable,
    log_validation_issues,
    raise_on_errors,
    validate_syntax_for_statements,
    validate_statements_remote,
)

__COMMON_ROOT = Path(__file__).resolve().parents[2]
__PROJECT_ROOT = __COMMON_ROOT.parent
configure(HarnessContext(harness_root=__COMMON_ROOT, project_root=__PROJECT_ROOT))


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


VALID_DDL = """CREATE TABLE IF NOT EXISTS publication_events (
    bookid BIGINT,
    author STRING,
    title STRING
) DISTRIBUTED BY HASH(bookid) INTO 1 BUCKETS
WITH (
    'changelog.mode' = 'append',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'value.format' = 'avro-registry'
);"""

VALID_DML = """INSERT INTO george_martin_books
SELECT bookid, author, title
FROM publication_events
WHERE author = 'George R. R. Martin';"""


def test_extract_ddl_header():
    sql_out = _extract_ddl_header(VALID_DDL)
    assert "DISTRIBUTED" not in sql_out
    assert "WITH" not in sql_out
    assert "changelog.mode" not in sql_out
    assert "kafka.retention.time" not in sql_out
    assert "kafka.producer.compression.type" not in sql_out
    assert "scan.bounded.mode" not in sql_out
    assert "scan.startup.mode" not in sql_out
    assert "value.fields-include" not in sql_out
    assert "value.format" not in sql_out
    valid_ddl_2 = """CREATE TABLE IF NOT EXISTS publication_events (
        bookid BIGINT,
        author STRING,
        title STRING
    )
    WITH (
        'changelog.mode' = 'append',
        'kafka.retention.time' = '0',
        'kafka.producer.compression.type' = 'snappy',
        'scan.bounded.mode' = 'unbounded',
        'scan.startup.mode' = 'earliest-offset',
        'value.fields-include' = 'all',
        'value.format' = 'avro-registry'
    );"""
    sql_out = _extract_ddl_header(valid_ddl_2)
    assert "WITH" not in sql_out


def test_validate_watermark_parseable():
    issues = _validate_parseable(VALID_DDL, "ddl", 0)
    assert not issues
    flink_ddl = """CREATE TABLE IF NOT EXISTS publication_events (
        bookid BIGINT,
        author STRING,
        title STRING,
        ts TIMESTAMP_LTZ(3),
        PRIMARY KEY (bookid) NOT ENFORCED,
         WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
    )
    DISTRIBUTED BY HASH(bookid) INTO 1 BUCKETS
    WITH (
        'changelog.mode' = 'append',
        'kafka.retention.time' = '0',
        'kafka.producer.compression.type' = 'snappy',
        'scan.bounded.mode' = 'unbounded',
        'scan.startup.mode' = 'earliest-offset',
        'value.fields-include' = 'all',
        'value.format' = 'avro-registry'
    )
    """
    issues = _validate_parseable(flink_ddl, "ddl", 0)
    assert not issues


def test_validate_virtual_metadata_parseable():
    doc_ex = """
    CREATE TABLE t (
        `user_id` BIGINT,
        `item_id` BIGINT,
        `behavior` STRING,
        `event_time` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
        `partition` BIGINT METADATA VIRTUAL,
        `offset` BIGINT METADATA VIRTUAL
    );
    """
    issues = _validate_parseable(doc_ex, "ddl", 0)
    assert not issues


def test_validate_dml_parseable():
    dml_ex = """
    INSERT INTO publication_events
    SELECT
      bookid,
      author,
      title,
      ts,
      JSON_VALUE(value, '$.patate') AS patate
    FROM publication_events
    WHERE author = 'George R. R. Martin';
    """
    issues = _validate_parseable(dml_ex, "dml", 0)
    assert not issues


def test_parse_error_message():
    exc = ParseError("parse failed", errors=[{"description": "Expected )", "line": 3}])
    message, line = _parse_error_message(exc)
    assert message == "Expected )"
    assert line == 3

    fallback_message, fallback_line = _parse_error_message(ParseError("plain failure"))
    assert fallback_message == "plain failure"
    assert fallback_line is None


def test_validate_one_rejects_non_create_ddl():
    issues = _validate_one("SELECT 1;", "ddl", 0)
    errors = [i for i in issues if i.severity == "error"]
    assert len(errors) == 1
    assert errors[0].message == "DDL must start with CREATE TABLE"


def test_validate_one_rejects_non_insert_dml():
    issues = _validate_one("SELECT 1 FROM t;", "dml", 0)
    errors = [i for i in issues if i.severity == "error"]
    assert len(errors) == 1
    assert errors[0].message == "DML must start with INSERT INTO"


def test_validate_one_empty_after_strip():
    assert _validate_one("-- only a comment", "ddl", 0) == []
    assert _validate_one("DROP TABLE foo;", "ddl", 0) == []


def test_validate_one_changelog_mode_warning():
    ddl = "CREATE TABLE t (id INT) WITH ('connector' = 'kafka');"
    issues = _validate_one(ddl, "ddl", 0)
    warnings = [i for i in issues if i.severity == "warning"]
    assert any("changelog.mode" in i.message for i in warnings)


@patch("flink_skill_common.sql_validate.get_logger")
def test_log_validation_issues(mock_get_logger):
    logger = MagicMock()
    mock_get_logger.return_value = logger
    issues = [
        SqlValidationIssue(0, "ddl", "bad ddl", severity="error"),
        SqlValidationIssue(1, "dml", "missing property", severity="warning"),
    ]
    log_validation_issues(issues)
    logger.error.assert_called_once_with(
        "SQL validation error [%s#%d]: %s",
        "ddl",
        0,
        "bad ddl",
    )
    logger.warning.assert_called_once_with(
        "SQL validation warning [%s#%d]: %s",
        "dml",
        1,
        "missing property",
    )


def test_raise_on_errors_ignores_warnings():
    issues = [
        SqlValidationIssue(0, "ddl", "missing changelog.mode", severity="warning"),
    ]
    raise_on_errors(issues)


def test_sql_validation_error_message():
    issues = [
        SqlValidationIssue(0, "ddl", "syntax error", line=4, severity="error"),
    ]
    err = SqlValidationError(issues)
    assert "syntax error" in str(err)
    assert "line 4" in str(err)
    assert err.issues == issues


@patch("flink_skill_common.deploy.flink_statement_manager.FlinkStatementManager")
@patch("flink_skill_common.config.flink_deploy_settings")
def test_validate_statements_remote(mock_settings, mock_manager_cls, settings):
    mock_settings.return_value = settings
    expected = [SqlValidationIssue(0, "ddl", "remote error")]
    mock_manager_cls.return_value.validate_statements.return_value = expected

    issues = validate_statements_remote([VALID_DDL], [VALID_DML])

    mock_manager_cls.assert_called_once_with(settings)
    mock_manager_cls.return_value.validate_statements.assert_called_once_with(
        [VALID_DDL],
        [VALID_DML],
    )
    assert issues == expected

def test_validate_statements_accepts_valid_fixture():
    issues = validate_syntax_for_statements([VALID_DDL], [VALID_DML])
    errors = [i for i in issues if i.severity == "error"]
    assert not errors


def test_validate_statements_empty_lists():
    assert validate_syntax_for_statements([], []) == []


def test_validate_statements_missing_parenthesis():
    issues = validate_syntax_for_statements(["CREATE TABLE t (id STRING"], [])
    errors = [i for i in issues if i.severity == "error"]
    assert errors
    assert errors[0].kind == "ddl"


def test_validate_statements_insert_typo():
    issues = validate_syntax_for_statements([], ["INSRT INTO t SELECT 1"])
    errors = [i for i in issues if i.severity == "error"]
    assert errors
    assert errors[0].kind == "dml"
    assert "INSERT INTO" in errors[0].message


def test_validate_statements_select_typo_in_dml():
    issues = validate_syntax_for_statements([], ["INSERT INTO t SELECT * FORM src"])
    errors = [i for i in issues if i.severity == "error"]
    assert errors


def test_raise_on_errors_raises():
    issues = validate_syntax_for_statements(["CREATE TABLE t (id STRING"], [])
    with pytest.raises(SqlValidationError):
        raise_on_errors(issues)


def test_validate_all_flink_references():
    ddls, dmls = load_all_valid_flink_reference_sql()
    assert len(ddls) >= 3
    assert len(dmls) >= 2

    issues = validate_syntax_for_statements(ddls, dmls)
    errors = [issue for issue in issues if issue.severity == "error"]
    assert not errors, "\n".join(
        f"[{issue.kind}#{issue.statement_index}] {issue.message}" for issue in errors
    )


