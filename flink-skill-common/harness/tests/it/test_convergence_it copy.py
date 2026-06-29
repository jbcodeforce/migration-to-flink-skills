"""Integration tests for converge_flink_sql workflow branches."""

from pathlib import Path

import pytest

from flink_fixtures import (
    assert_convergence_stages,
    assert_has_errors,
    assert_no_errors,
    load_flink_pair,
    load_source_sql,
    validation_issues,
)
from flink_skill_common.convergence import ConvergenceContext, converge_flink_sql
from flink_skill_common.sql_validate import SqlValidationError

pytestmark = pytest.mark.integration


TABLE_NAME = "filtered_publications"


def test_converge_valid_deploy(tmp_path: Path, require_deploy):
    ddls, dmls, src_dir = load_flink_pair("filtering", valid=True)

    ctx = ConvergenceContext(
        table_name=TABLE_NAME,
        source_sql=dmls[0],
        source_label="fixture",
        out_dir=tmp_path,
        tests_dir=src_dir / "tests",
    )
    result = converge_flink_sql(
        ddls,
        dmls,
        ctx,
        skip_deploy=False,
        agent_on_failure=False,
    )
    assert result.success is True
    assert any("Deploy OK" in msg for msg in result.messages)


def test_converge_invalid_raises_without_agent(tmp_path: Path):
    ddls, dmls, src_dir = load_flink_pair("ddl_bad_syntax", valid=False)
    ctx = ConvergenceContext(
        table_name=TABLE_NAME,
        source_sql="broken",
        source_label="fixture",
        out_dir=tmp_path,
        tests_dir=src_dir / "tests",
    )
    with pytest.raises(SqlValidationError):
        converge_flink_sql(ddls, dmls, ctx, skip_deploy=True, agent_on_failure=False)


@pytest.mark.integration_agent
def test_converge_agent_fixes_offline_error(tmp_path: Path, require_deploy, require_llm):
    """Single-tier smoke: offline sqlglot error only (skip_deploy=True)."""
    ddls, _ = load_flink_pair("ddl_bad_syntax", valid=False)
    valid_ddls, valid_dmls, src_dir = load_flink_pair("raw_classical_songs", valid=True)
    source_sql = load_source_sql("ddl_fixable_typo") or valid_ddls[0]

    ctx = ConvergenceContext(
        table_name=TABLE_NAME,
        source_sql=source_sql,
        source_label="fixture",
        out_dir=tmp_path,
        tests_dir=src_dir / "tests",
    )
    result = converge_flink_sql(
        ddls,
        [],
        ctx,
        skip_deploy=True,
        agent_on_failure=True,
    )
    assert result.last_agent_response
    remote_issues = validation_issues(result.ddls, result.dmls or valid_dmls, remote=True)
    if not result.success:
        assert_no_errors(remote_issues)


@pytest.mark.integration_agent
def test_converge_agent_fixes_remote_error(tmp_path: Path, require_deploy, require_llm):
    """Single-tier smoke: remote CC error only (skip_deploy=True)."""
    ddls, _ = load_flink_pair("ddl_missing_pk", valid=False)
    _, valid_dmls, src_dir = load_flink_pair("raw_classical_songs", valid=True)
    source_sql = load_source_sql("ddl_fixable_typo") or load_flink_pair("raw_classical_songs", valid=True)[0][0]

    offline_issues = validation_issues(ddls, [], remote=False)
    if any(i.severity == "error" for i in offline_issues):
        pytest.skip("ddl_missing_pk fails offline; cannot test remote-only agent branch")

    ctx = ConvergenceContext(
        table_name=TABLE_NAME,
        source_sql=source_sql,
        source_label="fixture",
        out_dir=tmp_path,
        tests_dir=src_dir / "tests",
    )
    result = converge_flink_sql(
        ddls,
        [],
        ctx,
        skip_deploy=True,
        agent_on_failure=True,
    )
    assert result.last_agent_response
    remote_issues = validation_issues(result.ddls, result.dmls or valid_dmls, remote=True)
    if not result.success:
        assert_no_errors(remote_issues)


@pytest.mark.integration_agent
def test_converge_agent_fixes_multi_error_sql(
    tmp_path: Path, require_deploy, require_llm, monkeypatch
):
    """Canonical full-loop demo: offline DML error, then remote DDL error, then deploy."""
    monkeypatch.setenv("AGENT_FIXER_EXECUTION_MAX_RETRIES", "3")

    ddls, dmls = load_flink_pair("multi_error_convergence", valid=False)
    valid_ddls, valid_dmls = load_flink_pair("raw_classical_songs", valid=True)
    source_sql = load_source_sql("multi_error_convergence") or valid_ddls[0]

    assert_has_errors(validation_issues(ddls, dmls, remote=False), kind="dml")
    assert_no_errors(validation_issues(ddls, [], remote=False))
    assert_has_errors(validation_issues(ddls, valid_dmls, remote=True), kind="ddl")

    ctx = ConvergenceContext(
        table_name=TABLE_NAME,
        source_sql=source_sql,
        source_label="fixture",
        out_dir=tmp_path,
    )
    result = converge_flink_sql(
        ddls,
        dmls,
        ctx,
        skip_deploy=False,
        agent_on_failure=True,
    )

    assert result.last_agent_response
    assert_convergence_stages(result.messages, expect_offline=True, expect_remote=True)
    assert_no_errors(validation_issues(result.ddls, result.dmls, remote=False))
    assert_no_errors(validation_issues(result.ddls, result.dmls, remote=True))
    assert result.success is True
    assert any("Deploy OK" in msg for msg in result.messages)
