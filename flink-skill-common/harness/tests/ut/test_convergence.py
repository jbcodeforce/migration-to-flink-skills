"""Unit tests for Flink SQL convergence loop."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from flink_skill_common.config import HarnessContext, configure

__COMMON_ROOT = Path(__file__).resolve().parents[2]
__PROJECT_ROOT = __COMMON_ROOT.parent
_HARNESS = HarnessContext(harness_root=__COMMON_ROOT, project_root=__PROJECT_ROOT)
configure(_HARNESS)

from flink_skill_common.convergence import (
    _apply_agent_fix,
    _deploy_messages,
    _format_validation_errors,
    ConvergenceContext,
    clean_flink_sql_and_validate,
    converge_flink_sql,
)
from flink_skill_common.deploy.flink_statement_manager import DeployError, DeployResult
from flink_skill_common.sql_validate import SqlValidationError, SqlValidationIssue

VALID_DDL = """CREATE TABLE IF NOT EXISTS my_table (
    id INT
) DISTRIBUTED BY HASH(id) INTO 1 BUCKETS
WITH ('changelog.mode' = 'append');"""

VALID_DML = "INSERT INTO my_table SELECT id FROM src;"

FIXED_RESPONSE = (
    "DDL\n```sql\n"
    + VALID_DDL
    + "\n```\nDML\n```sql\n"
    + VALID_DML
    + "\n```"
)

DEPLOY_OK = DeployResult(
    table_name="my_table",
    ddl_statement="my-table-ddl",
    dml_statement="my-table-dml",
    ddl_phase="COMPLETED",
    dml_phase="COMPLETED",
    success=True,
    messages=[],
)


@pytest.fixture
def ctx(tmp_path: Path) -> ConvergenceContext:
    return ConvergenceContext(
        table_name="my_table",
        source_sql="CREATE TABLE source (id INT);",
        source_label="fsql",
        out_dir=tmp_path,
    )


@pytest.fixture
def ctx_with_tests(ctx: ConvergenceContext, tmp_path: Path) -> ConvergenceContext:
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    return ConvergenceContext(
        table_name=ctx.table_name,
        source_sql=ctx.source_sql,
        source_label=ctx.source_label,
        out_dir=ctx.out_dir,
        tests_dir=tests_dir,
    )


def test_converge_skip_deploy_success(ctx: ConvergenceContext):
    result = converge_flink_sql(
        [VALID_DDL], [VALID_DML], ctx, skip_deploy=True, agent_on_failure=False
    )
    assert result.success is True
    assert result.ddl_path is not None
    assert "Offline validation passed." in result.messages
    assert "Skipped deploy" in result.messages[-1]


def test_converge_offline_validation_raises_without_agent(ctx: ConvergenceContext):
    with pytest.raises(SqlValidationError):
        converge_flink_sql(["CREATE TABLE t (id INT"], [], ctx, skip_deploy=True, agent_on_failure=False)


def test_converge_deploy_success(ctx_with_tests: ConvergenceContext):
    with patch("flink_skill_common.convergence.FlinkStatementManager") as mock_cls:
        mock_cls.return_value.deploy_table.return_value = DEPLOY_OK
        result = converge_flink_sql(
            [VALID_DDL], [VALID_DML], ctx_with_tests, agent_on_failure=False
        )

    assert result.success is True
    assert any("Deploy OK" in msg for msg in result.messages)
    mock_cls.return_value.deploy_table.assert_called_once_with(
        "my_table",
        result.ddl_path,
        result.dml_path,
        tests_dir=ctx_with_tests.tests_dir,
    )


def test_converge_deploy_error_without_agent(ctx_with_tests: ConvergenceContext):
    with patch("flink_skill_common.convergence.FlinkStatementManager") as mock_cls:
        mock_cls.return_value.deploy_table.side_effect = DeployError("DDL failed")
        result = converge_flink_sql(
            [VALID_DDL], [VALID_DML], ctx_with_tests, agent_on_failure=False
        )

    assert result.success is False
    assert any("DDL failed" in msg for msg in result.messages)


def test_converge_agent_fix_on_offline_error(ctx_with_tests: ConvergenceContext):
    bad_issue = SqlValidationIssue(
        statement_index=0, kind="ddl", message="syntax error", severity="error"
    )

    with patch("flink_skill_common.convergence.validate_syntax_for_statements") as mock_validate:
        mock_validate.side_effect = [
            [bad_issue],
            [],
        ]
        with patch(
            "flink_skill_common.convergence.run_agent_deploy_fixer",
            return_value=FIXED_RESPONSE,
        ):
            with patch("flink_skill_common.convergence.FlinkStatementManager") as mock_cls:
                mock_cls.return_value.deploy_table.return_value = DEPLOY_OK
                with patch(
                    "flink_skill_common.convergence.agent_fixer_max_retries",
                    return_value=2,
                ):
                    result = converge_flink_sql(
                        ["CREATE TABLE bad"],
                        [],
                        ctx_with_tests,
                        agent_on_failure=True,
                    )

    assert result.success is True
    assert result.last_agent_response == FIXED_RESPONSE


def test_converge_agent_fix_on_deploy_error(ctx_with_tests: ConvergenceContext):
    with patch("flink_skill_common.convergence.FlinkStatementManager") as mock_cls:
        mock_cls.return_value.deploy_table.side_effect = [
            DeployError("DDL failed"),
            DEPLOY_OK,
        ]
        with patch(
            "flink_skill_common.convergence.run_agent_deploy_fixer",
            return_value=FIXED_RESPONSE,
        ):
            with patch(
                "flink_skill_common.convergence.agent_fixer_max_retries",
                return_value=2,
            ):
                result = converge_flink_sql(
                    [VALID_DDL],
                    [VALID_DML],
                    ctx_with_tests,
                    agent_on_failure=True,
                )

    assert result.success is True
    assert any("Deploy failed, invoking agent fix" in msg for msg in result.messages)


def test_converge_deploy_unhealthy_without_agent(ctx_with_tests: ConvergenceContext):
    unhealthy = DeployResult(
        table_name="my_table",
        ddl_statement="my-table-ddl",
        dml_statement="my-table-dml",
        ddl_phase="FAILED",
        dml_phase="SKIPPED",
        success=False,
        messages=["deploy failed"],
    )
    with patch("flink_skill_common.convergence.FlinkStatementManager") as mock_cls:
        mock_cls.return_value.deploy_table.return_value = unhealthy
        result = converge_flink_sql(
            [VALID_DDL], [VALID_DML], ctx_with_tests, agent_on_failure=False
        )

    assert result.success is False
    assert any("Deploy unhealthy" in msg for msg in result.messages)


def test_converge_agent_fix_on_unhealthy_deploy(ctx_with_tests: ConvergenceContext):
    unhealthy = DeployResult(
        table_name="my_table",
        ddl_statement="my-table-ddl",
        dml_statement="my-table-dml",
        ddl_phase="FAILED",
        dml_phase="SKIPPED",
        success=False,
        exceptions="timeout",
        messages=[],
    )

    with patch("flink_skill_common.convergence.FlinkStatementManager") as mock_cls:
        mock_cls.return_value.deploy_table.side_effect = [unhealthy, DEPLOY_OK]
        with patch(
            "flink_skill_common.convergence.run_agent_deploy_fixer",
            return_value=FIXED_RESPONSE,
        ) as mock_fixer:
            with patch(
                "flink_skill_common.convergence.agent_fixer_max_retries",
                return_value=2,
            ):
                result = converge_flink_sql(
                    [VALID_DDL],
                    [VALID_DML],
                    ctx_with_tests,
                    agent_on_failure=True,
                )


    assert result.success is True
    assert any("exceptions=timeout" in msg for msg in result.messages)
    mock_fixer.assert_called_once()
    assert "exceptions=timeout" in mock_fixer.call_args.kwargs["error_message"]


def test_converge_no_ddl_for_agent_fix(ctx: ConvergenceContext, tmp_path: Path):
    bad_issue = SqlValidationIssue(
        statement_index=0, kind="ddl", message="syntax error", severity="error"
    )
    dml_path = tmp_path / "dml.my_table.sql"

    with patch("flink_skill_common.convergence.validate_syntax_for_statements", return_value=[bad_issue]):
        with patch(
            "flink_skill_common.convergence._resolve_paths",
            return_value=(None, dml_path),
        ):
            result = converge_flink_sql(
                ["CREATE TABLE bad"],
                [],
                ctx,
                agent_on_failure=True,
            )

    assert result.success is False
    assert result.messages == ["No DDL file found for agent fix"]
    assert result.ddl_path is None
    assert result.dml_path == dml_path


def test_converge_no_ddl_for_deploy(ctx: ConvergenceContext, tmp_path: Path):
    dml_path = tmp_path / "dml.my_table.sql"

    with patch("flink_skill_common.convergence.validate_syntax_for_statements", return_value=[]):
        with patch(
            "flink_skill_common.convergence._resolve_paths",
            return_value=(None, dml_path),
        ):
            result = converge_flink_sql(
                [VALID_DDL],
                [VALID_DML],
                ctx,
                agent_on_failure=False,
            )

    assert result.success is False
    assert result.messages == [
        "Offline validation passed.",
        "No tests directory found, skipping deploy",
    ]
    assert result.ddl_path is None
    assert result.dml_path == dml_path


def test_converge_exhausts_retries(ctx: ConvergenceContext):
    bad_issue = SqlValidationIssue(
        statement_index=0, kind="ddl", message="syntax error", severity="error"
    )
    with patch("flink_skill_common.convergence.validate_syntax_for_statements", return_value=[bad_issue]):
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


def test_deploy_messages_success():
    result = DeployResult(
        table_name="my_table",
        ddl_statement="ddl-stmt",
        dml_statement="dml-stmt",
        ddl_phase="COMPLETED",
        dml_phase="COMPLETED",
        success=True,
        messages=["existing"],
        source_statements=[("src_table", "COMPLETED")],
    )
    messages = _deploy_messages(result)
    assert messages[0] == "existing"
    assert "Source DDL OK: src_table (COMPLETED)" in messages
    assert "Deploy OK: ddl-stmt (COMPLETED), dml-stmt (COMPLETED)" in messages


def test_deploy_messages_unhealthy():
    result = DeployResult(
        table_name="my_table",
        ddl_statement="ddl-stmt",
        dml_statement="dml-stmt",
        ddl_phase="FAILED",
        dml_phase="SKIPPED",
        success=False,
        messages=["partial"],
    )
    messages = _deploy_messages(result)
    assert messages == ["partial", "Deploy unhealthy: DDL=FAILED DML=SKIPPED"]


def test_apply_agent_fix_updates_only_changed_blocks(ctx: ConvergenceContext, tmp_path: Path):
    ddl_path = tmp_path / "ddl.my_table.sql"
    dml_path = tmp_path / "dml.my_table.sql"
    ddl_path.write_text(VALID_DDL)
    dml_path.write_text(VALID_DML)
    original_ddls = [VALID_DDL]
    new_dml = "INSERT INTO my_table SELECT 1;"

    with patch(
        "flink_skill_common.convergence.run_agent_deploy_fixer",
        return_value="fixed",
    ):
        with patch(
            "flink_skill_common.convergence.extract_sql_blocks",
            return_value=([], [new_dml]),
        ):
            ddls, dmls, response = _apply_agent_fix(
                ctx,
                ddl_path,
                dml_path,
                "error",
                original_ddls,
                [VALID_DML],
            )

    assert ddls == original_ddls
    assert dmls == [new_dml]
    assert response == "fixed"


def test_apply_agent_fix_writes_source_ddls(ctx: ConvergenceContext, tmp_path: Path):
    ddl_path = tmp_path / "ddl.my_table.sql"
    dml_path = tmp_path / "dml.my_table.sql"
    ddl_path.write_text(VALID_DDL)
    dml_path.write_text(VALID_DML)
    source_ddls = {"src": "CREATE TABLE src (id INT);"}

    with patch(
        "flink_skill_common.convergence.run_agent_deploy_fixer",
        return_value="fixed",
    ):
        with patch(
            "flink_skill_common.convergence.extract_sql_blocks",
            return_value=([], []),
        ):
            with patch(
                "flink_skill_common.convergence.parse_source_ddls_from_response",
                return_value=source_ddls,
            ):
                with patch(
                    "flink_skill_common.convergence.write_source_ddls",
                ) as mock_write:
                    _apply_agent_fix(
                        ctx,
                        ddl_path,
                        dml_path,
                        "error",
                        [VALID_DDL],
                        [VALID_DML],
                    )

    mock_write.assert_called_once_with(ctx.out_dir, source_ddls)


def test_apply_agent_fix_tests_dir_fallback(ctx: ConvergenceContext, tmp_path: Path):
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    ddl_path = tmp_path / "ddl.my_table.sql"
    dml_path = tmp_path / "dml.my_table.sql"
    ddl_path.write_text(VALID_DDL)
    dml_path.write_text(VALID_DML)

    with patch(
        "flink_skill_common.convergence.run_agent_deploy_fixer",
        return_value="fixed",
    ) as mock_fixer:
        with patch(
            "flink_skill_common.convergence.extract_sql_blocks",
            return_value=([], []),
        ):
            _apply_agent_fix(
                ctx,
                ddl_path,
                dml_path,
                "error",
                [VALID_DDL],
                [VALID_DML],
            )

    assert mock_fixer.call_args.kwargs["tests_dir"] == tests_dir


def test_format_validation_errors_includes_line():
    issues = [
        SqlValidationIssue(
            statement_index=0,
            kind="ddl",
            message="bad syntax",
            severity="error",
            line=5,
        ),
        SqlValidationIssue(
            statement_index=1,
            kind="dml",
            message="minor",
            severity="warning",
        ),
    ]
    formatted = _format_validation_errors(issues)
    assert formatted == "[ddl#0] bad syntax (line 5)"


@patch("flink_skill_common.convergence.converge_flink_sql")
@patch("flink_skill_common.config.agent_fixer_enabled", return_value=False)
def test_clean_flink_sql_and_validate_no_dml(mock_agent, mock_converge, tmp_path: Path):
    response = """
DDL:
```sql
CREATE TABLE IF NOT EXISTS t (id INT);
```
"""
    result = clean_flink_sql_and_validate(
        response,
        "t",
        "CREATE TABLE src (id INT);",
        skip_deploy=True,
        out_dir=tmp_path,
    )
    assert result is None
    mock_converge.assert_not_called()


@patch("flink_skill_common.convergence.converge_flink_sql")
@patch("flink_skill_common.config.agent_fixer_enabled", return_value=False)
def test_clean_flink_sql_and_validate_converges(mock_agent, mock_converge, tmp_path: Path):
    mock_converge.return_value = MagicMock(success=True)
    response = """
DDL:
```sql
CREATE TABLE IF NOT EXISTS src (id INT);
CREATE TABLE IF NOT EXISTS target (id INT);
```

DML:
```sql
INSERT INTO target SELECT id FROM src;
```
"""
    result = clean_flink_sql_and_validate(
        response,
        "target",
        "CREATE TABLE src (id INT);",
        skip_deploy=True,
        out_dir=tmp_path,
    )
    assert result is not None
    mock_converge.assert_called_once()
    table_dir = tmp_path / "target"
    assert (table_dir / "ddl.target.sql").is_file()
    assert (table_dir / "dml.target.sql").is_file()


@patch("flink_skill_common.convergence.converge_flink_sql")
@patch("flink_skill_common.convergence.generate_source_ddls")
@patch("flink_skill_common.config.agent_fixer_enabled", return_value=False)
def test_clean_flink_sql_and_validate_generates_missing_sources(
    mock_agent,
    mock_generate,
    mock_converge,
    tmp_path: Path,
):
    mock_converge.return_value = MagicMock(success=True)
    mock_generate.return_value = {
        "publication_events": "CREATE TABLE IF NOT EXISTS publication_events (id INT);",
    }
    response = """
DDL:
```sql
CREATE TABLE IF NOT EXISTS george_martin_books (bookid BIGINT);
```

DML:
```sql
INSERT INTO george_martin_books SELECT bookid FROM publication_events;
```
"""
    clean_flink_sql_and_validate(
        response,
        "george_martin_books",
        "CREATE STREAM all_publications (bookid BIGINT);",
        skip_deploy=True,
        out_dir=tmp_path,
    )
    mock_generate.assert_called_once()
    source_path = tmp_path / "george_martin_books" / "tests" / "ddl.publication_events.sql"
    assert source_path.is_file()
