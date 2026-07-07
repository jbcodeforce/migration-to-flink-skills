"""CLI entry point for Spark → Flink migration."""

from __future__ import annotations

from pathlib import Path

import typer

from flink_skill_common.cli_interrupt import MIGRATION_INTERRUPT_EXIT_CODE, run_typer_app
from flink_skill_common.agents.factory import resolve_llm_model
from flink_skill_common.config import llm_reachable
from flink_skill_common.convergence import clean_flink_sql_and_validate
from flink_skill_common.sql_validate import SqlValidationError
import spark_flink_skill.config  # noqa: F401 — configure shared harness context
from spark_flink_skill.agents.migrate_agent import MigrationError, run_migration
from spark_flink_skill.sql_utils import clean_sql_input, detect_tables

app = typer.Typer(add_completion=False, no_args_is_help=True)


@app.command()
def migrate(
    table: str = typer.Option(..., "--table", "-t", help="Target Flink table name"),
    file: Path = typer.Option(..., "--file", "-f", help="Spark SQL source file"),
    out_dir: Path = typer.Option(Path("output"), "--out-dir", "-o", help="Output directory"),
    skip_deploy: bool = typer.Option(
        False, "--skip-deploy", help="Translate only; do not deploy to CC Flink."
    ),
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
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    typer.echo(f"Using model: {resolved_model}")

    cleaned = clean_sql_input(file.read_text())
    detection = detect_tables(cleaned)
    statements = detection.table_statements if detection.has_multiple_tables else [cleaned]

    try:
        for index, stmt in enumerate(statements, start=1):
            if not stmt.strip():
                continue
            typer.echo(f"[{index}/{len(statements)}] Translating...")
            try:
                response = run_migration(table, stmt)
            except MigrationError as exc:
                typer.echo(f"Migration failed: {exc}", err=True)
                raise typer.Exit(1) from exc

            typer.echo("Extracting SQL blocks and validating...")
            result = clean_flink_sql_and_validate(
                response,
                table,
                stmt,
                skip_deploy,
                out_dir,
            )
            if result is None:
                typer.echo("Output files written (no DML)")
            elif not result.success:
                typer.echo("\n".join(result.messages), err=True)
                raise typer.Exit(1)
            else:
                detail = result.ddl_path.name if result.ddl_path is not None else ""
                if skip_deploy:
                    typer.echo(f"Offline validation passed{f' ({detail})' if detail else ''}")
                else:
                    typer.echo(f"Deploy succeeded{f' ({detail})' if detail else ''}")

        typer.echo(f"\nDone. Output: {out_dir.resolve()}")
    except KeyboardInterrupt:
        typer.echo("\nMigration interrupted.", err=True)
        raise typer.Exit(MIGRATION_INTERRUPT_EXIT_CODE) from None
    except typer.Exit:
        raise
    except SqlValidationError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


def main() -> None:
    run_typer_app(app)


if __name__ == "__main__":
    main()
