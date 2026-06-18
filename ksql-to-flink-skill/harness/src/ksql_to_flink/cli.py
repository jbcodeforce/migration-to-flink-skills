"""CLI entry point for ksqlDB → Flink migration."""

from __future__ import annotations

from pathlib import Path

import typer

from flink_skill_common.logging_config import configure_cli_logging
logger=configure_cli_logging("ksql_to_flink.cli")

from flink_skill_common.config import agent_deploy_on_failure as agent_deploy_on_failure_env
from flink_skill_common.llm import llm_reachable
from .deploy import DeployError, FlinkStatementManager, require_flink_deploy_ready
from flink_skill_common.logging_config import configure_cli_logging
from flink_skill_common.output import extract_sql_blocks, resolve_table_paths, write_output, write_source_ddls
from flink_skill_common.sql_validate import (
    SqlValidationError,
    log_validation_issues,
    raise_on_errors,
    validate_statements,
    validate_statements_remote,
)

from .sources import generate_source_ddls
from .sql_utils import (
    clean_ksql_input,
    compute_missing_source_tables
)
from .agents.migrate_agent import run_agent_deploy_retry, run_migration
from flink_skill_common.config import configure, HarnessContext

_HARNESS_ROOT = Path(__file__).resolve().parents[2]
_PROJECT_ROOT = _HARNESS_ROOT.parent

configure(HarnessContext(harness_root=_HARNESS_ROOT, project_root=_PROJECT_ROOT))   

app = typer.Typer(add_completion=False, no_args_is_help=True)


def _run_deploy(
    table: str,
    ddl_path: Path,
    dml_path: Path,
    ksql: str,
    agent_on_failure: bool,
    tests_dir: Path | None,
) -> None:
    require_flink_deploy_ready()
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


@app.command()
def migrate(
    table: str = typer.Option(..., "--table", "-t"),
    file: Path = typer.Option(..., "--file", "-f"),
    out_dir: Path = typer.Option(Path("output"), "--out-dir", "-o"),
    skip_deploy: bool = typer.Option(False, "--skip-deploy", help="Translate only; do not deploy to CC Flink."),
    skip_flink_validate: bool = typer.Option(
        False,
        "--skip-flink-validate",
        help="Skip tier-2 CC Flink parser validation before deploy.",
    ),
    agent_deploy_on_failure: bool = typer.Option(
        False,
        "--agent-deploy-on-failure",
        help="On deploy failure, invoke Agno agent with confluent-sql tools to fix and redeploy.",
    ),
) -> None:
    """Migrate a ksqlDB file to Flink DDL and DML, 
    then deploy to Confluent Cloud is enabled."""
    logger.info(
        "migrate start table=%s file=%s out_dir=%s skip_deploy=%s",
        table,
        file,
        out_dir,
        skip_deploy,
    )
    try:
        if not file.exists():
            logger.error("File not found: %s (cwd=%s)", file.resolve(), Path.cwd())
            typer.echo(f"File not found: {file}", err=True)
            raise typer.Exit(1)
        if not llm_reachable():
            logger.error("LLM not reachable")
            typer.echo("LLM not reachable. Start oMLX or set SL_LLM_BASE_URL.", err=True)
            raise typer.Exit(1)

        cleaned = clean_ksql_input(file.read_text())
        logger.debug("Cleaned ksql length=%d chars", len(cleaned))
        response = run_migration(table, cleaned)
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
            typer.echo(f"Wrote {path}")

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
                source_ddls = generate_source_ddls(table, cleaned, dml_sql, missing)
                source_paths = write_source_ddls(out_dir, source_ddls)
                for path in source_paths:
                    typer.echo(f"Wrote {path}")
                tests_dir = out_dir / "tests"

        if skip_deploy:
            logger.info("Skipped deploy (--skip-deploy)")
            typer.echo("Skipped deploy (--skip-deploy).")
            return

        if ddl_path is None:
            typer.echo(f"No DDL file found for table {table!r}", err=True)
            raise typer.Exit(1)

        if not skip_flink_validate:
            remote_issues = validate_statements_remote(ddls, dmls)
            log_validation_issues(remote_issues)
            raise_on_errors(remote_issues)

        agent_deploy_on_failure = agent_deploy_on_failure or agent_deploy_on_failure_env()
        if tests_dir is None and (out_dir / "tests").is_dir():
            tests_dir = out_dir / "tests"
        _run_deploy(table, ddl_path, dml_path, cleaned, agent_deploy_on_failure, tests_dir)
    except typer.Exit:
        raise
    except SqlValidationError as exc:
        logger.error("SQL validation failed: %s", exc)
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    except Exception as exc:
        logger.exception("migrate failed table=%s file=%s", table, file)
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc


if __name__ == "__main__":
    app()
