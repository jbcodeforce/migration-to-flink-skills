"""CLI entry point for ksqlDB → Flink migration."""

from __future__ import annotations

from pathlib import Path

import typer

from ksql_flink_skill.agents.migrate_agent import run_agent_deploy_retry, run_migration
from ksql_flink_skill.config import agent_deploy_on_failure as agent_deploy_on_failure_env
from ksql_flink_skill.deploy import DeployError, deploy_table, require_flink_deploy_ready
from ksql_flink_skill.output import extract_sql_blocks, write_output, write_source_ddls
from ksql_flink_skill.sources import generate_source_ddls
from ksql_flink_skill.sql_utils import (
    clean_ksql_input,
    compute_missing_source_tables,
    llm_reachable,
)

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
    try:
        result = deploy_table(table, ddl_path, dml_path, tests_dir=tests_dir)
    except DeployError as exc:
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
        typer.echo(msg)
    if not result.success:
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
    agent_deploy_on_failure: bool = typer.Option(
        False,
        "--agent-deploy-on-failure",
        help="On deploy failure, invoke Agno agent with confluent-sql tools to fix and redeploy.",
    ),
) -> None:
    """Migrate a ksqlDB file to Flink DDL and DML, then deploy to Confluent Cloud."""
    if not file.exists():
        typer.echo(f"File not found: {file}", err=True)
        raise typer.Exit(1)
    if not llm_reachable():
        typer.echo("LLM not reachable. Start oMLX or set SL_LLM_BASE_URL.", err=True)
        raise typer.Exit(1)

    cleaned = clean_ksql_input(file.read_text())
    response = run_migration(table, cleaned)
    ddl, dml = extract_sql_blocks(response)

    ddls = [ddl] if ddl.strip() else []
    dmls = [dml] if dml.strip() else []
    ddl_path, dml_path = write_output(table, ddls, dmls, out_dir)
    typer.echo(f"Wrote {ddl_path}")
    typer.echo(f"Wrote {dml_path}")

    tests_dir: Path | None = None
    if dml.strip():
        missing = compute_missing_source_tables(dml, table, ddl)
        if missing:
            typer.echo(f"Generating source DDL stubs for: {', '.join(missing)}")
            source_ddls = generate_source_ddls(table, cleaned, dml, missing)
            source_paths = write_source_ddls(out_dir, source_ddls)
            for path in source_paths:
                typer.echo(f"Wrote {path}")
            tests_dir = out_dir / "tests"

    if skip_deploy:
        typer.echo("Skipped deploy (--skip-deploy).")
        return

    use_agent = agent_deploy_on_failure or agent_deploy_on_failure_env()
    if tests_dir is None and (out_dir / "tests").is_dir():
        tests_dir = out_dir / "tests"
    _run_deploy(table, ddl_path, dml_path, cleaned, use_agent, tests_dir)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
