"""
Copyright 2024-2026 Confluent, Inc.
KSQL to Flink SQL Translation Agent
CLI entry point for ksqlDB → Flink migration.
"""

from __future__ import annotations

from pathlib import Path

import typer

from flink_skill_common.config import (
    configure,
    HarnessContext,
    cli_log_file,
    get_logger,
    llm_base_url,
)
from flink_skill_common.llm import llm_reachable
from flink_skill_common.sql_validate import SqlValidationError

from .ksql_utils import (
    clean_ksql_input,
    extract_ksql_object_name,
    split_ksql_create_statements,
)
from .migrate_agent import run_migration
from .pipeline import clean_flink_sql_and_validate


_HARNESS_ROOT = Path(__file__).resolve().parents[2]
_PROJECT_ROOT = _HARNESS_ROOT.parent

_context = HarnessContext(harness_root=_HARNESS_ROOT, project_root=_PROJECT_ROOT)
configure(_context)

logger = get_logger()

app = typer.Typer(add_completion=False, no_args_is_help=True)


@app.command()
def migrate(
    table: str = typer.Option(..., "--table", "-t"),
    file: Path = typer.Option(..., "--file", "-f"),
    out_dir: Path = typer.Option(Path("output"), "--out-dir", "-o"),
    skip_deploy: bool = typer.Option(False, "--skip-deploy", help="Translate only; do not deploy to CC Flink."),
) -> None:
    """Migrate ksqlDB CREATE statements to Flink DDL/DML, one statement at a time."""
    logger.info(
        "migrate start table=%s file=%s out_dir=%s skip_deploy=%s",
        table,
        file,
        out_dir,
        skip_deploy,
    )
    typer.echo(f"Log file: {cli_log_file()}")
    try:
        if not file.exists():
            logger.error("File not found: %s (cwd=%s)", file.resolve(), Path.cwd())
            typer.echo(f"File not found: {file}", err=True)
            raise typer.Exit(1)

        typer.echo(f"Checking LLM at {llm_base_url()} ...")
        if not llm_reachable():
            base = llm_base_url()
            logger.error("LLM not reachable at %s", base)
            typer.echo(
                f"LLM not reachable at {base}/models. "
                "Check SL_LLM_BASE_URL, VPN, and that the server is running.",
                err=True,
            )
            raise typer.Exit(1)
        typer.echo("LLM reachable.")

        ksql_statements = split_ksql_create_statements(file.read_text())
        if not ksql_statements:
            typer.echo("No CREATE STREAM/TABLE statements found in file.", err=True)
            raise typer.Exit(1)

        total = len(ksql_statements)
        typer.echo(f"Found {total} CREATE statement(s); migrating one at a time.")

        for index, ksql_statement in enumerate(ksql_statements, start=1):
            ksql_cleaned = clean_ksql_input(ksql_statement)
            if not ksql_cleaned.strip():
                logger.warning("Skipping empty statement index=%d", index)
                continue

            source_name = extract_ksql_object_name(ksql_cleaned) or f"statement_{index}"
            typer.echo(
                f"[{index}/{total}] Migrating ksql {source_name!r} "
                f"-> Flink target {table!r} ({len(ksql_cleaned)} chars) ..."
            )
            logger.info(
                "Migrating statement %d/%d source=%s target=%s",
                index,
                total,
                source_name,
                table,
            )

            response = run_migration(table, ksql_cleaned, source_name=source_name)
            clean_flink_sql_and_validate(response, table, ksql_cleaned, skip_deploy, out_dir)
            typer.echo(f"[{index}/{total}] Finished {source_name!r}.")

        typer.echo(f"Done. Processed {total} statement(s). Output: {out_dir.resolve()}")

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


def main():
    app()


if __name__ == "__main__":
    main()
