"""CLI entry point for Spark → Flink migration."""

from __future__ import annotations

from pathlib import Path

import typer

from spark_flink_skill.agents.migrate_agent import MigrationError, run_migration
from spark_flink_skill.output import extract_sql_blocks, resolve_table_paths, write_output
from flink_skill_common.sql_validate import (
    SqlValidationError,
    log_validation_issues,
    raise_on_errors,
    validate_statements,
)
from spark_flink_skill.sql_utils import (
    LlmConfigError,
    clean_sql_input,
    detect_tables,
    ensure_model_context,
    llm_reachable,
    resolve_llm_model,
)

app = typer.Typer(add_completion=False, no_args_is_help=True)


@app.command()
def migrate(
    table: str = typer.Option(..., "--table", "-t", help="Target Flink table name"),
    file: Path = typer.Option(..., "--file", "-f", help="Spark SQL source file"),
    out_dir: Path = typer.Option(Path("output"), "--out-dir", "-o", help="Output directory"),
) -> None:
    """Migrate a Spark SQL file to Flink DDL and DML."""
    if not file.exists():
        typer.echo(f"File not found: {file}", err=True)
        raise typer.Exit(1)
    if not llm_reachable():
        typer.echo(
            "LLM not reachable. Start oMLX or set SL_LLM_BASE_URL in the repo-root .env (or DOTENV_FILE)",
            err=True,
        )
        raise typer.Exit(1)
    try:
        resolved_model = resolve_llm_model()
        ensure_model_context(resolved_model)
    except LlmConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    typer.echo(f"Using model: {resolved_model}")

    cleaned = clean_sql_input(file.read_text())
    detection = detect_tables(cleaned)
    statements = detection.table_statements if detection.has_multiple_tables else [cleaned]

    ddls: list[str] = []
    dmls: list[str] = []
    for stmt in statements:
        try:
            response = run_migration(table, stmt)
        except MigrationError as exc:
            typer.echo(f"Migration failed: {exc}", err=True)
            raise typer.Exit(1) from exc
        stmt_ddls, stmt_dmls = extract_sql_blocks(response)
        ddls.extend(stmt_ddls)
        dmls.extend(stmt_dmls)

    if not ddls and not dmls:
        typer.echo(
            "Migration produced no DDL or DML. Check SL_LLM_MODEL context window "
            "and agent output format.",
            err=True,
        )
        raise typer.Exit(1)

    try:
        offline_issues = validate_statements(ddls, dmls)
        log_validation_issues(offline_issues)
        raise_on_errors(offline_issues)
    except SqlValidationError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc

    ddl_paths, dml_paths = write_output(table, ddls, dmls, out_dir)
    for path in ddl_paths + dml_paths:
        typer.echo(f"Wrote {path}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
