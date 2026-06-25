"""Integration tests for converge_flink_sql workflow branches."""

from pathlib import Path

import pytest

from flink_fixtures import (
    assert_no_errors,
    load_pair,
    load_source_sql,
    validation_issues,
)
from flink_skill_common.convergence import ConvergenceContext, converge_flink_sql
from flink_skill_common.sql_validate import SqlValidationError

pytestmark = pytest.mark.integration

TABLE_NAME = "raw_classical_songs"


def _valid_context(out_dir: Path) -> ConvergenceContext:
    _, dmls = load_pair("raw_classical_songs", valid=True)
    return ConvergenceContext(
        table_name=TABLE_NAME,
        source_sql="CREATE STREAM raw_classical_songs (artist STRING, title STRING);",
        source_label="fixture",
        out_dir=out_dir,
    )


def test_converge_valid_skip_deploy(tmp_path: Path):
    ddls, dmls = load_pair("raw_classical_songs", valid=True)
    result = converge_flink_sql(
        ddls,
        dmls,
        _valid_context(tmp_path),
        skip_deploy=True,
        agent_on_failure=False,
    )
    print(result)
    assert result.success is True
    assert result.ddl_path is not None
    assert any("Skipped deploy" in msg for msg in result.messages)


def test_converge_valid_deploy(tmp_path: Path, require_deploy):
    ddls, dmls = load_pair("raw_classical_songs", valid=True)
    result = converge_flink_sql(
        ddls,
        dmls,
        _valid_context(tmp_path),
        skip_deploy=False,
        agent_on_failure=False,
    )
    assert result.success is True
    assert any("Deploy OK" in msg for msg in result.messages)


def test_converge_invalid_raises_without_agent(tmp_path: Path):
    ddls, dmls = load_pair("ddl_bad_syntax", valid=False)
    ctx = ConvergenceContext(
        table_name=TABLE_NAME,
        source_sql="broken",
        source_label="fixture",
        out_dir=tmp_path,
    )
    with pytest.raises(SqlValidationError):
        converge_flink_sql(ddls, dmls, ctx, skip_deploy=True, agent_on_failure=False)


@pytest.mark.integration_agent
def test_converge_agent_fixes_offline_error(tmp_path: Path, require_deploy, require_llm):
    ddls, _ = load_pair("ddl_bad_syntax", valid=False)
    valid_ddls, valid_dmls = load_pair("raw_classical_songs", valid=True)
    source_sql = load_source_sql("ddl_fixable_typo") or valid_ddls[0]

    ctx = ConvergenceContext(
        table_name=TABLE_NAME,
        source_sql=source_sql,
        source_label="fixture",
        out_dir=tmp_path,
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
    ddls, _ = load_pair("ddl_missing_pk", valid=False)
    _, valid_dmls = load_pair("raw_classical_songs", valid=True)
    source_sql = load_source_sql("ddl_fixable_typo") or load_pair("raw_classical_songs", valid=True)[0][0]

    offline_issues = validation_issues(ddls, [], remote=False)
    if any(i.severity == "error" for i in offline_issues):
        pytest.skip("ddl_missing_pk fails offline; cannot test remote-only agent branch")

    ctx = ConvergenceContext(
        table_name=TABLE_NAME,
        source_sql=source_sql,
        source_label="fixture",
        out_dir=tmp_path,
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
