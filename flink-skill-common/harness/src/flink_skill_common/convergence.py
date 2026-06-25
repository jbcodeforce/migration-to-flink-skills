"""
Convergence loop on Flink SQL validation: 
1- sqlglot validate 
2- remote validate with CC deployment
3- agent fix.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from flink_skill_common.agents.deploy_fixer import run_agent_deploy_fixer
from flink_skill_common.config import agent_fixer_enabled, agent_fixer_max_retries, get_logger
from flink_skill_common.deploy.flink_statement_manager import DeployError, FlinkStatementManager
from flink_skill_common.output import (
    extract_sql_blocks,
    parse_source_ddls_from_response,
    resolve_table_paths,
    write_output,
    write_source_ddls,
)
from flink_skill_common.sql_validate import (
    SqlValidationError,
    SqlValidationIssue,
    log_validation_issues,
    validate_statements,
    validate_statements_remote,
)


def _logger():
    return get_logger()


@dataclass(frozen=True)
class ConvergenceContext:
    table_name: str
    source_sql: str
    source_label: str
    out_dir: Path
    tests_dir: Path | None = None


@dataclass
class ConvergenceResult:
    success: bool
    ddls: list[str]
    dmls: list[str]
    ddl_path: Path | None
    dml_path: Path | None
    messages: list[str] = field(default_factory=list)
    last_agent_response: str | None = None


def _format_validation_errors(issues: list[SqlValidationIssue]) -> str:
    errors = [issue for issue in issues if issue.severity == "error"]
    return "\n".join(
        f"[{issue.kind}#{issue.statement_index}] {issue.message}"
        + (f" (line {issue.line})" if issue.line else "")
        for issue in errors
    )


def _resolve_paths(
    table_name: str,
    ddls: list[str],
    dmls: list[str],
    out_dir: Path,
) -> tuple[Path | None, Path | None]:
    ddl_paths, dml_paths = write_output(table_name, ddls, dmls, out_dir)
    ddl_path, dml_path = resolve_table_paths(ddl_paths, dml_paths, table_name)
    if dml_path is None:
        dml_path = out_dir / f"dml.{table_name}.sql"
    return ddl_path, dml_path


def _apply_agent_fix(
    ctx: ConvergenceContext,
    ddl_path: Path,
    dml_path: Path,
    error_message: str,
    ddls: list[str],
    dmls: list[str],
) -> tuple[list[str], list[str], str]:
    tests_dir = ctx.tests_dir
    if tests_dir is None and (ctx.out_dir / "tests").is_dir():
        tests_dir = ctx.out_dir / "tests"

    response = run_agent_deploy_fixer(
        table_name=ctx.table_name,
        source_sql=ctx.source_sql,
        source_label=ctx.source_label,
        ddl_path=ddl_path,
        dml_path=dml_path,
        error_message=error_message,
        tests_dir=tests_dir,
    )

    new_ddls, new_dmls = extract_sql_blocks(response)
    if new_ddls:
        ddls = new_ddls
    if new_dmls:
        dmls = new_dmls

    source_ddls = parse_source_ddls_from_response(response)
    if source_ddls:
        write_source_ddls(ctx.out_dir, source_ddls)

    return ddls, dmls, response


def _deploy_messages(result) -> list[str]:
    messages = list(result.messages)
    if result.success:
        for src_name, src_phase in result.source_statements:
            messages.append(f"Source DDL OK: {src_name} ({src_phase})")
        messages.append(
            f"Deploy OK: {result.ddl_statement} ({result.ddl_phase}), "
            f"{result.dml_statement or 'no DML'} ({result.dml_phase or 'skipped'})"
        )
    else:
        messages.append(
            f"Deploy unhealthy: DDL={result.ddl_phase} DML={result.dml_phase}"
        )
    return messages

#
# Public API
# ---------

def converge_flink_sql(
    ddls: list[str],
    dmls: list[str],
    ctx: ConvergenceContext,
    *,
    skip_deploy: bool = False,
    agent_on_failure: bool | None = None,
) -> ConvergenceResult:
    """Loop validation, deploy, and agent fix until SQL converges or retries exhaust."""
    use_agent = agent_fixer_enabled() if agent_on_failure is None else agent_on_failure
    max_attempts = agent_fixer_max_retries() if use_agent else 1

    current_ddls = list(ddls)
    current_dmls = list(dmls)
    messages: list[str] = []
    last_agent_response: str | None = None
    ddl_path: Path | None = None
    dml_path: Path | None = None

    for attempt in range(max_attempts):
        _logger().info("Convergence attempt %d of %d for table=%s", attempt + 1, max_attempts, ctx.table_name)

        offline_issues = validate_statements(current_ddls, current_dmls)
        log_validation_issues(offline_issues)
        offline_errors = [i for i in offline_issues if i.severity == "error"]
        if offline_errors:
            if not use_agent:
                raise SqlValidationError(offline_errors)
            ddl_path, dml_path = _resolve_paths(ctx.table_name, current_ddls, current_dmls, ctx.out_dir)
            if ddl_path is None:
                return ConvergenceResult(
                    success=False,
                    ddls=current_ddls,
                    dmls=current_dmls,
                    ddl_path=None,
                    dml_path=dml_path,
                    messages=["No DDL file found for agent fix"],
                )
            messages.append(f"Offline validation failed, invoking agent fix (attempt {attempt + 1})")
            current_ddls, current_dmls, last_agent_response = _apply_agent_fix(
                ctx,
                ddl_path,
                dml_path,
                _format_validation_errors(offline_issues),
                current_ddls,
                current_dmls,
            )
            continue

        ddl_path, dml_path = _resolve_paths(ctx.table_name, current_ddls, current_dmls, ctx.out_dir)

        if skip_deploy:
            messages.append("Skipped deploy (--skip-deploy).")
            return ConvergenceResult(
                success=True,
                ddls=current_ddls,
                dmls=current_dmls,
                ddl_path=ddl_path,
                dml_path=dml_path,
                messages=messages,
                last_agent_response=last_agent_response,
            )
        # Deploy to CC backend
        if ddl_path is None:
            return ConvergenceResult(
                success=False,
                ddls=current_ddls,
                dmls=current_dmls,
                ddl_path=None,
                dml_path=dml_path,
                messages=["No DDL file found for table"],
            )

        remote_issues = validate_statements_remote(current_ddls, current_dmls)
        log_validation_issues(remote_issues)
        remote_errors = [i for i in remote_issues if i.severity == "error"]
        if remote_errors:
            if not use_agent:
                raise SqlValidationError(remote_errors)
            messages.append(f"Remote validation failed, invoking agent fix (attempt {attempt + 1})")
            current_ddls, current_dmls, last_agent_response = _apply_agent_fix(
                ctx,
                ddl_path,
                dml_path,
                _format_validation_errors(remote_issues),
                current_ddls,
                current_dmls,
            )
            continue

        tests_dir = ctx.tests_dir
        if tests_dir is None and (ctx.out_dir / "tests").is_dir():
            tests_dir = ctx.out_dir / "tests"

        _logger().info("Deploying table=%s ddl=%s dml=%s", ctx.table_name, ddl_path, dml_path)
        try:
            result = FlinkStatementManager().deploy_table(
                ctx.table_name, ddl_path, dml_path, tests_dir=tests_dir
            )
        except DeployError as exc:
            _logger().error("Deploy failed for table=%s: %s", ctx.table_name, exc, exc_info=True)
            if not use_agent:
                messages.append(str(exc))
                return ConvergenceResult(
                    success=False,
                    ddls=current_ddls,
                    dmls=current_dmls,
                    ddl_path=ddl_path,
                    dml_path=dml_path,
                    messages=messages,
                )
            messages.append(f"Deploy failed, invoking agent fix: {exc}")
            current_ddls, current_dmls, last_agent_response = _apply_agent_fix(
                ctx,
                ddl_path,
                dml_path,
                str(exc),
                current_ddls,
                current_dmls,
            )
            continue

        messages.extend(_deploy_messages(result))
        if result.success:
            return ConvergenceResult(
                success=True,
                ddls=current_ddls,
                dmls=current_dmls,
                ddl_path=ddl_path,
                dml_path=dml_path,
                messages=messages,
                last_agent_response=last_agent_response,
            )

        if not use_agent:
            return ConvergenceResult(
                success=False,
                ddls=current_ddls,
                dmls=current_dmls,
                ddl_path=ddl_path,
                dml_path=dml_path,
                messages=messages,
            )

        error_message = (
            f"DDL={result.ddl_phase} DML={result.dml_phase}"
            + (f" exceptions={result.exceptions}" if result.exceptions else "")
        )
        messages.append(f"Deploy unhealthy, invoking agent fix: {error_message}")
        current_ddls, current_dmls, last_agent_response = _apply_agent_fix(
            ctx,
            ddl_path,
            dml_path,
            error_message,
            current_ddls,
            current_dmls,
        )

    return ConvergenceResult(
        success=False,
        ddls=current_ddls,
        dmls=current_dmls,
        ddl_path=ddl_path,
        dml_path=dml_path,
        messages=messages,
        last_agent_response=last_agent_response,
    )
