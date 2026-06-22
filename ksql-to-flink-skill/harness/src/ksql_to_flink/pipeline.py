"""Post-migration validation, output writing, and optional deploy."""

from __future__ import annotations

from pathlib import Path

import typer

from flink_skill_common.config import agent_fixer_enabled, get_logger
from flink_skill_common.deploy.flink_statement_manager import DeployError, FlinkStatementManager
from flink_skill_common.output import (
    extract_sql_blocks,
    resolve_table_paths,
    write_output,
    write_source_ddls,
)
from flink_skill_common.sql_validate import (
    log_validation_issues,
    raise_on_errors,
    validate_statements,
    validate_statements_remote,
)

from .ksql_utils import compute_missing_source_tables
from .migrate_agent import run_agent_deploy_retry
from .sources import generate_source_ddls

logger = get_logger()


def _run_deploy(
    table: str,
    ddl_path: Path,
    dml_path: Path,
    ksql: str,
    agent_on_failure: bool,
    tests_dir: Path | None,
) -> None:
    logger.info("Deploying table=%s ddl=%s dml=%s", table, ddl_path, dml_path)
    try:
        result = FlinkStatementManager().deploy_table(table, ddl_path, dml_path, tests_dir=tests_dir)
    except DeployError as exc:
        logger.error("Deploy failed for table=%s: %s", table, exc, exc_info=True)
        if agent_on_failure:
            typer.echo(f"Deploy failed, invoking agent retry: {exc}", err=True)
            retry_result = run_agent_deploy_retry(
                table_name=table,
                ksql=ksql,
                ddl_path=ddl_path,
                dml_path=dml_path,
                error_message=str(exc),
                tests_dir=tests_dir,
            )
            typer.echo(retry_result)
            return
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc

    for msg in result.messages:
        logger.info("Deploy message: %s", msg)
        typer.echo(msg)
    if not result.success:
        logger.error(
            "Deploy unhealthy for table=%s: DDL=%s DML=%s exceptions=%s",
            table,
            result.ddl_phase,
            result.dml_phase,
            result.exceptions,
        )
        typer.echo(f"Deploy unhealthy: DDL={result.ddl_phase} DML={result.dml_phase}", err=True)
        raise typer.Exit(1)
    for src_name, src_phase in result.source_statements:
        typer.echo(f"Source DDL OK: {src_name} ({src_phase})")
    typer.echo(
        f"Deploy OK: {result.ddl_statement} ({result.ddl_phase}), "
        f"{result.dml_statement or 'no DML'} ({result.dml_phase or 'skipped'})"
    )


def clean_flink_sql_and_validate(
    response: str,
    table: str,
    src_ksql: str,
    skip_deploy: bool,
    out_dir: Path,
) -> tuple[Path, Path] | None:
    ddls, dmls = extract_sql_blocks(response)
    logger.info(
        "Extracted ddl=%d statements dml=%d statements",
        len(ddls),
        len(dmls),
    )

    offline_issues = validate_statements(ddls, dmls)
    log_validation_issues(offline_issues)
    raise_on_errors(offline_issues)

    ddl_paths, dml_paths = write_output(table, ddls, dmls, out_dir)
    for path in ddl_paths + dml_paths:
        logger.info("Wrote %s", path)

    ddl_path, dml_path = resolve_table_paths(ddl_paths, dml_paths, table)
    if dml_path is None:
        dml_path = out_dir / f"dml.{table}.sql"

    tests_dir: Path | None = None
    if dmls:
        dml_sql = "\n\n".join(dmls)
        ddl_sql = "\n\n".join(ddls)
        missing = compute_missing_source_tables(dml_sql, table, ddl_sql)
        if missing:
            typer.echo(f"Generating source DDL stubs for: {', '.join(missing)}")
            source_ddls = generate_source_ddls(table, src_ksql, dml_sql, missing)
            source_paths = write_source_ddls(out_dir, source_ddls)
            for path in source_paths:
                typer.echo(f"Wrote {path}")
            tests_dir = out_dir / "tests"

    if skip_deploy:
        logger.info("Skipped deploy (--skip-deploy)")
        typer.echo("Skipped deploy (--skip-deploy).")
        return None

    if ddl_path is None:
        typer.echo(f"No DDL file found for table {table!r}", err=True)
        raise typer.Exit(1)

    remote_issues = validate_statements_remote(ddls, dmls)
    log_validation_issues(remote_issues)
    raise_on_errors(remote_issues)

    if tests_dir is None and (out_dir / "tests").is_dir():
        tests_dir = out_dir / "tests"
    _run_deploy(table, ddl_path, dml_path, src_ksql, agent_fixer_enabled(), tests_dir)
    return ddl_path, dml_path
