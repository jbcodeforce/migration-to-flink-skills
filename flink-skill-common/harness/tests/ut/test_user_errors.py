from flink_skill_common.deploy.flink_statement_manager import (
    DeployError,
    StatementManagerError,
)
from flink_skill_common.user_errors import format_agent_retry_message, format_user_error


class _RootError(Exception):
    pass


def test_format_user_error_returns_root_cause_message():
    root = _RootError(
        "Statement submission failed: Schema Registry subject 'clicks-key' doesn't match."
    )
    wrapped = StatementManagerError("Failed to create clicks-ddl: wrapper detail")
    wrapped.__cause__ = root

    assert format_user_error(wrapped) == (
        "Statement submission failed: Schema Registry subject 'clicks-key' doesn't match."
    )


def test_format_user_error_strips_failed_to_create_prefix():
    exc = StatementManagerError(
        "Failed to create clicks-ddl: Cannot create table because the Schema Registry "
        "subject 'clicks-key' doesn't match the existing one."
    )

    assert format_user_error(exc) == (
        "Cannot create table because the Schema Registry subject 'clicks-key' "
        "doesn't match the existing one."
    )


def test_format_user_error_deploy_error():
    exc = DeployError("Source DDL clicks-ddl failed with phase FAILED: timeout")

    assert format_user_error(exc) == (
        "Source DDL clicks-ddl failed with phase FAILED: timeout"
    )


def test_format_agent_retry_message():
    msg = format_agent_retry_message("Deploy failed: schema mismatch.", 1, 2)

    assert msg == (
        "Deploy failed: schema mismatch. Agent fixer will attempt to correct SQL "
        "automatically (attempt 1/2)."
    )
