"""Unit tests for Flink SQL convergence loop."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from flink_skill_common.config import HarnessContext, configure

__COMMON_ROOT = Path(__file__).resolve().parents[2]
__PROJECT_ROOT = __COMMON_ROOT.parent
_HARNESS = HarnessContext(harness_root=__COMMON_ROOT, project_root=__PROJECT_ROOT)
configure(_HARNESS)

from flink_skill_common.convergence import(
    _apply_agent_fix,
    _deploy_messages,
    ConvergenceContext,
    converge_flink_sql,
)
from flink_skill_common.deploy.flink_statement_manager import DeployError, DeployResult
from flink_skill_common.sql_validate import SqlValidationError, SqlValidationIssue

VALID_DDL = """CREATE TABLE IF NOT EXISTS my_table (
    id INT
) DISTRIBUTED BY HASH(id) INTO 1 BUCKETS
WITH ('changelog.mode' = 'append');"""

VALID_DML = "INSERT INTO my_table SELECT id FROM src;"


@pytest.fixture
def ctx(tmp_path: Path) -> ConvergenceContext:
    return ConvergenceContext(
        table_name="my_table",
        source_sql="CREATE TABLE source (id INT);",
        source_label="fsql",
        out_dir=tmp_path,
    )


def test_converge_skip_deploy_success(ctx: ConvergenceContext):
    result = converge_flink_sql([VALID_DDL], [VALID_DML], ctx, skip_deploy=True)
    assert result.success is True
    assert result.ddl_path is not None
    assert "Skipped deploy" in result.messages[-1]
    print(result)


def test_converge_offline_validation_raises_without_agent(ctx: ConvergenceContext):
    with pytest.raises(SqlValidationError):
        converge_flink_sql(["CREATE TABLE t (id INT"], [], ctx, skip_deploy=True, agent_on_failure=False)


def test_converge_deploy_success(ctx: ConvergenceContext):
    deploy_result = DeployResult(
        table_name="my_table",
        ddl_statement="my-table-ddl",
        dml_statement="my-table-dml",
        ddl_phase="COMPLETED",
        dml_phase="COMPLETED",
        success=True,
        messages=["created"],
    )
    with patch("flink_skill_common.convergence.validate_statements_remote", return_value=[]):
        with patch("flink_skill_common.convergence.FlinkStatementManager") as mock_cls:
            mock_cls.return_value.deploy_table.return_value = deploy_result
            result = converge_flink_sql([VALID_DDL], [VALID_DML], ctx, agent_on_failure=False)

    assert result.success is True
    assert any("Deploy OK" in msg for msg in result.messages)
    mock_cls.return_value.deploy_table.assert_called_once()


def test_converge_deploy_error_without_agent(ctx: ConvergenceContext):
    with patch("flink_skill_common.convergence.validate_statements_remote", return_value=[]):
        with patch("flink_skill_common.convergence.FlinkStatementManager") as mock_cls:
            mock_cls.return_value.deploy_table.side_effect = DeployError("DDL failed")
            result = converge_flink_sql([VALID_DDL], [VALID_DML], ctx, agent_on_failure=False)

    assert result.success is False
    assert any("DDL failed" in msg for msg in result.messages)


def test_converge_agent_fix_on_offline_error(ctx: ConvergenceContext):
    bad_issue = SqlValidationIssue(
        statement_index=0, kind="ddl", message="syntax error", severity="error"
    )
    fixed_response = (
        "DDL\n```sql\n"
        + VALID_DDL
        + "\n```\nDML\n```sql\n"
        + VALID_DML
        + "\n```"
    )
    deploy_result = DeployResult(
        table_name="my_table",
        ddl_statement="my-table-ddl",
        dml_statement="my-table-dml",
        ddl_phase="COMPLETED",
        dml_phase="COMPLETED",
        success=True,
        messages=[],
    )

    with patch("flink_skill_common.convergence.validate_statements") as mock_validate:
        mock_validate.side_effect = [
            [bad_issue],
            [],
        ]
        with patch(
            "flink_skill_common.convergence.run_agent_deploy_fixer",
            return_value=fixed_response,
        ):
            with patch("flink_skill_common.convergence.validate_statements_remote", return_value=[]):
                with patch("flink_skill_common.convergence.FlinkStatementManager") as mock_cls:
                    mock_cls.return_value.deploy_table.return_value = deploy_result
                    with patch(
                        "flink_skill_common.convergence.agent_fixer_max_retries",
                        return_value=2,
                    ):
                        result = converge_flink_sql(
                            ["CREATE TABLE bad"],
                            [],
                            ctx,
                            agent_on_failure=True,
                        )

    assert result.success is True
    assert result.last_agent_response == fixed_response


def test_converge_exhausts_retries(ctx: ConvergenceContext):
    bad_issue = SqlValidationIssue(
        statement_index=0, kind="ddl", message="syntax error", severity="error"
    )
    with patch("flink_skill_common.convergence.validate_statements", return_value=[bad_issue]):
        with patch(
            "flink_skill_common.convergence.run_agent_deploy_fixer",
            return_value="still broken",
        ):
            with patch(
                "flink_skill_common.convergence.extract_sql_blocks",
                return_value=([], []),
            ):
                with patch(
                    "flink_skill_common.convergence.agent_fixer_max_retries",
                    return_value=1,
                ):
                    result = converge_flink_sql(
                        ["CREATE TABLE bad"],
                        [],
                        ctx,
                        agent_on_failure=True,
                    )

    assert result.success is False
