"""
Copyright 2024-2026 Confluent, Inc.
KSQL to Flink SQL Translation Agent
CLI entry point for ksqlDB → Flink migration.
"""

from __future__ import annotations

from pathlib import Path

import typer

import ksql_to_flink.config  # noqa: F401 — configure shared harness context
from flink_skill_common.config import (
    agent_fixer_enabled,
    agent_fixer_max_retries,
    cli_log_file,
    get_logger,
    llm_base_url,
    llm_reachable,
    skill_dir,
)
from flink_skill_common.agents.factory import resolve_llm_model
from flink_skill_common.convergence import ConvergenceResult, clean_flink_sql_and_validate
from flink_skill_common.user_errors import format_user_error
from flink_skill_common.sql_validate import SqlValidationError
from flink_skill_common.cli_interrupt import MIGRATION_INTERRUPT_EXIT_CODE, run_typer_app
from flink_skill_common.cli_progress import ProgressReporter
from ksql_to_flink.migrate_agent import run_migration
from .ksql_utils import clean_ksql_input, extract_ksql_object_name, split_ksql_create_statements

app = typer.Typer(add_completion=False, no_args_is_help=True)


def _discover_statement_names(statements: list[str]) -> list[str]:
    names: list[str] = []
    for index, statement in enumerate(statements, start=1):
        cleaned = clean_ksql_input(statement)
        if not cleaned.strip():
            continue
        names.append(extract_ksql_object_name(cleaned) or f"statement_{index}")
    return names


def _convergence_failure_message(result: ConvergenceResult) -> str:
    for msg in reversed(result.messages):
        if "Agent fixer will attempt" in msg:
            return msg.split(" Agent fixer will attempt", 1)[0]
        if msg.startswith(("Deploy failed:", "Validation failed:", "Deploy unhealthy:")):
            return msg
    return result.messages[-1] if result.messages else "Validation or deploy failed."


def _on_convergence_progress(progress: ProgressReporter, msg: str) -> None:
    if "Agent fixer will attempt" in msg:
        progress.warn(msg)
    else:
        progress.sub(msg)

        
@app.command()
def migrate(
    table: str = typer.Option(..., "--table", "-t"),
    file: Path = typer.Option(..., "--file", "-f"),
    out_dir: Path = typer.Option(Path("output"), "--out-dir", "-o"),
    skip_deploy: bool = typer.Option(False, "--skip-deploy", help="Translate only; do not deploy to CC Flink."),
) -> None:
    """Migrate ksqlDB CREATE statements to Flink DDL/DML, one statement at a time."""
    logger = get_logger()
    logger.info("skill_dir=%s", skill_dir())
    progress = ProgressReporter()
    logger.info(
        "migrate start table=%s file=%s out_dir=%s skip_deploy=%s",
        table,
        file,
        out_dir,
        skip_deploy,
    )
    try:
        progress.banner(
            table=table,
            file=str(file),
            out_dir=str(out_dir),
            deploy="skipped" if skip_deploy else "enabled",
            model=resolve_llm_model(),
            agent_fixer="enabled" if agent_fixer_enabled() else "disabled",
            agent_fixer_retries=str(agent_fixer_max_retries()),
            log=str(cli_log_file()),
        )

        if not file.exists():
            logger.error("File not found: %s (cwd=%s)", file.resolve(), Path.cwd())
            typer.echo(f"File not found: {file}", err=True)
            raise typer.Exit(1)

        progress.step(1, f"Checking LLM at {llm_base_url()} ...")
        if not llm_reachable():
            base = llm_base_url()
            logger.error("LLM not reachable at %s", base)
            typer.echo(
                f"LLM not reachable at {base}/models. "
                "Check SL_LLM_BASE_URL, VPN, and that the server is running.",
                err=True,
            )
            raise typer.Exit(1)
        progress.done(1, f"LLM reachable at {llm_base_url()}")
        src_ksql_text = file.read_text()
        ksql_statements = split_ksql_create_statements(src_ksql_text)
        if not ksql_statements:
            typer.echo("No CREATE STREAM/TABLE statements found in file.", err=True)
            raise typer.Exit(1)

        statement_names = _discover_statement_names(ksql_statements)
        total = len(statement_names)
        names_summary = ", ".join(statement_names)
        progress.done(
            2,
            f"Found {total} CREATE statement(s)",
            names_summary,
        )

        processed = 0
        try:
            for index, ksql_statement in enumerate(ksql_statements, start=1):
                ksql_cleaned = clean_ksql_input(ksql_statement)
                if not ksql_cleaned.strip():
                    logger.warning("Skipping empty statement index=%d", index)
                    continue

                source_name = extract_ksql_object_name(ksql_cleaned) or f"statement_{index}"
                processed += 1
                progress.header(f"[{processed}/{total}] {source_name} → {table}")

                logger.info(
                    "Migrating statement %d/%d source=%s target=%s",
                    processed,
                    total,
                    source_name,
                    table,
                )

                progress.done(1, "Cleaned ksql input", f"{len(ksql_cleaned)} chars")

                progress.step(2, "Running translation agent...")
                response = run_migration(
                    table_name=table,
                    ksql=ksql_cleaned,
                    src_ksql=src_ksql_text,
                    source_name=source_name,
                    on_event=progress.agent_event,
                )
                progress.done(2, "Translation agent finished", f"{len(response)} chars")
                progress.step(3, "Extracting SQL blocks and validating...")
                result = clean_flink_sql_and_validate(
                    response,
                    table,
                    ksql_cleaned,
                    skip_deploy,
                    out_dir,
                    on_progress=lambda msg: _on_convergence_progress(progress, msg),
                )
                if result is None:
                    progress.done(3, "Output files written", "no DML")
                elif not result.success:
                    failure_msg = _convergence_failure_message(result)
                    logger.error("Migration failed for table=%s: %s", table, failure_msg)
                    progress.done(3, "Validation failed")
                    progress.warn(failure_msg)
                    typer.echo(failure_msg, err=True)
                    raise typer.Exit(1)
                else:
                    detail = ""
                    if result.ddl_path is not None:
                        detail = result.ddl_path.name
                    progress.done(3, "Validation finished", detail)
                    if skip_deploy:
                        progress.done(4, "Offline validation passed")
                    else:
                        progress.done(5, "Deploy succeeded")

            typer.echo(
                f"\nDone. Processed {processed} statement(s). Output: {out_dir.resolve()}"
            )
        except KeyboardInterrupt:
            source_name = locals().get("source_name", "unknown")
            logger.warning(
                "Migration interrupted at statement %d/%d (%s)",
                processed,
                total,
                source_name,
            )
            typer.echo(
                f"\nInterrupted during {source_name} ({processed}/{total}).",
                err=True,
            )
            raise typer.Exit(MIGRATION_INTERRUPT_EXIT_CODE) from None

    except typer.Exit:
        raise
    except SqlValidationError as exc:
        logger.error("SQL validation failed: %s", exc)
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    except Exception as exc:
        logger.exception("migrate failed table=%s file=%s", table, file)
        typer.echo(f"Error: {format_user_error(exc)}", err=True)
        raise typer.Exit(1) from exc


def main() -> None:
    get_logger()
    run_typer_app(app)


if __name__ == "__main__":
    main()
