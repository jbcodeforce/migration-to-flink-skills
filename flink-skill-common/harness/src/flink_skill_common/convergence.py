"""
Convergence loop on Flink SQL validation: 
1- sqlglot validate 
2- remote validate with CC deployment
3- agent fix.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from flink_skill_common.agents.cc_deployment_fixer import run_agent_deploy_fixer
from flink_skill_common.agents.table_source_agent import generate_source_ddls
from flink_skill_common.config import agent_fixer_enabled, agent_fixer_max_retries, get_logger
from flink_skill_common.deploy.flink_statement_manager import DeployError, FlinkStatementManager
from flink_skill_common.response_io import (
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
    validate_syntax_for_statements
)
from flink_skill_common.sql_parse import compute_missing_source_tables
from flink_skill_common.user_errors import format_agent_retry_message, format_user_error


def _logger():
    return get_logger()


def _emit_progress(on_progress: Callable[[str], None] | None, message: str) -> None:
    _logger().info("%s", message)
    if on_progress is not None:
        on_progress(message)


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


def _first_validation_error(issues: list[SqlValidationIssue]) -> str:
    errors = [issue for issue in issues if issue.severity == "error"]
    if not errors:
        return "Validation failed."
    issue = errors[0]
    line = f" (line {issue.line})" if issue.line else ""
    return f"[{issue.kind}#{issue.statement_index}] {issue.message}{line}"


def _notify_agent_retry(
    on_progress: Callable[[str], None] | None,
    *,
    prefix: str,
    detail: str,
    attempt: int,
    max_attempts: int,
    messages: list[str],
) -> None:
    user_detail = detail.strip()
    if prefix == "Deploy failed":
        user_detail = format_user_error(DeployError(detail))
    user_msg = format_agent_retry_message(f"{prefix}: {user_detail}", attempt, max_attempts)
    log_msg = f"{prefix}, invoking agent fix (attempt {attempt}): {detail}"
    if not messages or messages[-1] != log_msg:
        messages.append(log_msg)
    _logger().info(log_msg)
    _emit_progress(on_progress, user_msg)


def _apply_agent_fix(
    ctx: ConvergenceContext,
    ddl_path: Path,
    dml_path: Path,
    error_message: str,
    ddls: list[str],
    dmls: list[str] | None,
    *,
    on_progress: Callable[[str], None] | None = None,
) -> tuple[list[str], list[str], str]:
    tests_dir = ctx.tests_dir
    if tests_dir is None and (ctx.out_dir / "tests").is_dir():
        tests_dir = ctx.out_dir / "tests"

    _emit_progress(on_progress, "Invoking deploy fixer agent...")
    response = run_agent_deploy_fixer(
        table_name=ctx.table_name,
        source_sql=ctx.source_sql,
        source_label=ctx.source_label,
        ddl_path=ddl_path,
        dml_path=dml_path,
        error_message=error_message,
        tests_dir=tests_dir,
        on_event=on_progress,
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

manager_singleton: FlinkStatementManager | None = None
def _manager() -> FlinkStatementManager:
        global manager_singleton
        if manager_singleton is None:
            manager_singleton = FlinkStatementManager()
        return manager_singleton

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
    on_progress: Callable[[str], None] | None = None,
) -> ConvergenceResult:
    """
    Loop validation, deploy, and agent fix until SQL converge to successful deployments or retries exhaust.
    """
    use_agent = agent_fixer_enabled() if agent_on_failure is None else agent_on_failure
    max_attempts = agent_fixer_max_retries() if use_agent else 1

    current_ddls = list(ddls)
    current_dmls = list(dmls)
    messages: list[str] = []
    last_agent_response: str | None = None
    ddl_path: Path | None = None
    dml_path: Path | None = None
    deploy_attempted = False

    try:
        for attempt in range(max_attempts):
            _logger().info("Convergence attempt %d of %d for table=%s", attempt + 1, max_attempts, ctx.table_name)
            if max_attempts > 1:
                _emit_progress(on_progress, f"Convergence attempt {attempt + 1} of {max_attempts}")

            offline_issues = validate_syntax_for_statements(current_ddls, current_dmls)
            log_validation_issues(offline_issues)
            offline_errors = [i for i in offline_issues if i.severity == "error"]
            ddl_path, dml_path = _resolve_paths(ctx.table_name, current_ddls, current_dmls, ctx.out_dir)
            if offline_errors:
                error_messages = [msg for err in offline_errors for msg in err.message]
                if not use_agent:
                    raise SqlValidationError(offline_errors)
                if ddl_path is None:
                    return ConvergenceResult(
                        success=False,
                        ddls=current_ddls,
                        dmls=current_dmls,
                        ddl_path=None,
                        dml_path=dml_path,
                        messages=error_messages,
                    )
                _notify_agent_retry(
                    on_progress,
                    prefix="Validation failed",
                    detail=_first_validation_error(offline_issues),
                    attempt=attempt + 1,
                    max_attempts=max_attempts,
                    messages=error_messages,
                )
                current_ddls, current_dmls, last_agent_response = _apply_agent_fix(
                    ctx,
                    ddl_path,
                    dml_path,
                    _format_validation_errors(offline_issues),
                    current_ddls,
                    current_dmls,
                    on_progress=on_progress,
                )
                continue

            messages.append("Offline validation passed.")
            _emit_progress(on_progress, messages[-1])

            if skip_deploy:
                messages.append("Skipped deploy (--skip-deploy).")
                _emit_progress(on_progress, messages[-1])
                return ConvergenceResult(
                    success=True,
                    ddls=current_ddls,
                    dmls=current_dmls,
                    ddl_path=ddl_path,
                    dml_path=dml_path,
                    messages=messages,
                    last_agent_response=last_agent_response,
                )
            # Deploy to CC backend - Need to create inputs so dml will succeed
            if ctx.tests_dir is None:
                messages.append("No tests directory found, skipping deploy")
                _emit_progress(on_progress, messages[-1])
                return ConvergenceResult(
                    success=False,
                    ddls=current_ddls,
                    dmls=current_dmls,
                    ddl_path=ddl_path,
                    dml_path=dml_path,
                    messages=messages,
                )

            _logger().info("Deploying table=%s ddl=%s dml=%s", ctx.table_name, ddl_path, dml_path)
            _emit_progress(on_progress, "Deploying to Confluent Cloud Flink...")
            deploy_attempted = True
            try:
                result = _manager().deploy_table(
                    ctx.table_name, ddl_path, dml_path, tests_dir=ctx.tests_dir
                )
            except DeployError as exc:
                _logger().error("Deploy failed for table=%s: %s", ctx.table_name, exc, exc_info=True)
                if not use_agent:
                    messages.append(format_user_error(exc))
                    return ConvergenceResult(
                        success=False,
                        ddls=current_ddls,
                        dmls=current_dmls,
                        ddl_path=ddl_path,
                        dml_path=dml_path,
                        messages=messages,
                    )
                _notify_agent_retry(
                    on_progress,
                    prefix="Deploy failed",
                    detail=str(exc),
                    attempt=attempt + 1,
                    max_attempts=max_attempts,
                    messages=messages,
                )
                current_ddls, current_dmls, last_agent_response = _apply_agent_fix(
                    ctx,
                    ddl_path,
                    dml_path,
                    str(exc),
                    current_ddls,
                    current_dmls,
                    on_progress=on_progress,
                )
                continue

            deploy_messages = _deploy_messages(result)
            messages.extend(deploy_messages)
            for msg in deploy_messages:
                _emit_progress(on_progress, msg)
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
            _notify_agent_retry(
                on_progress,
                prefix="Deploy unhealthy",
                detail=error_message,
                attempt=attempt + 1,
                max_attempts=max_attempts,
                messages=messages,
            )
            current_ddls, current_dmls, last_agent_response = _apply_agent_fix(
                ctx,
                ddl_path,
                dml_path,
                error_message,
                current_ddls,
                current_dmls,
                on_progress=on_progress,
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
    finally:
        if deploy_attempted and not skip_deploy and ctx.tests_dir is not None:
            _logger().info("Cleaning up deployed statements for table=%s", ctx.table_name)
            _emit_progress(on_progress, "Cleaning up Confluent Cloud statements...")
            _manager().cleanup_deployed_table(ctx.table_name, ctx.tests_dir)


def clean_flink_sql_and_validate(
    response: str,  # migrated sql response from LLM may include DDLs and DMLs
    table: str,
    src_sql: str,  # source SQL before translation
    skip_deploy: bool,
    out_dir: Path,
    *,
    on_progress: Callable[[str], None] | None = None,
) -> ConvergenceResult | None:
    """
    From the LLM response, extract DDL/DML, write output, and run convergence.
    Returns ConvergenceResult when DML is present, otherwise None.
    """
    ddls, dmls = extract_sql_blocks(response)
    _emit_progress(on_progress, f"\nExtracted {len(ddls)} DDL, {len(dmls)} DML")
    table_dir = out_dir / table
    table_dir.mkdir(parents=True, exist_ok=True)
    tests_dir = table_dir / "tests"
    tests_dir.mkdir(parents=True, exist_ok=True)
    _emit_progress(on_progress, f"Writing output files to {table_dir}")
    write_output(table, ddls, dmls, table_dir)

    if not dmls:
        _emit_progress(on_progress, "No DML in response; skipped validation and deploy")
        return None

    dml_sql = "\n\n".join(dmls)
    ddl_sql = "\n\n".join(ddls)
    missing = compute_missing_source_tables(dml_sql, table, ddl_sql)
    if missing:
        _emit_progress(on_progress, f"Missing source tables: {', '.join(missing)}")
        _emit_progress(on_progress, "Generating source DDL stubs...")
        source_ddls = generate_source_ddls(table, src_sql, dml_sql, missing)
        write_source_ddls(table_dir, source_ddls)
        _emit_progress(on_progress, f"Wrote {len(source_ddls)} source DDL stub(s)")

    return converge_flink_sql(
        ddls,
        dmls,
        ConvergenceContext(
            table_name=table,
            source_sql=src_sql,
            source_label="source_sql",
            out_dir=table_dir,
            tests_dir=tests_dir,
        ),
        skip_deploy=skip_deploy,
        agent_on_failure=agent_fixer_enabled(),
        on_progress=on_progress,
    )
