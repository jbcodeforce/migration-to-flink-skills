"""
Copyright 2024-2026 Confluent, Inc.
KSQL to Flink SQL Translation Agent
CLI entry point for ksqlDB → Flink migration.
"""

from __future__ import annotations

from pathlib import Path

import typer
from flink_skill_common.config import (
    agent_fixer_enabled,
    agent_fixer_max_retries,
    cli_log_file,
    get_logger,
    llm_base_url,
    llm_reachable,
    skill_dir,
    configure,
    HarnessContext
)
from flink_skill_common.agents.factory import resolve_llm_model
from flink_skill_common.convergence import ConvergenceResult, clean_flink_sql_and_validate
from flink_skill_common.user_errors import format_user_error
from flink_skill_common.sql_validate import SqlValidationError
from flink_skill_common.cli_interrupt import MIGRATION_INTERRUPT_EXIT_CODE, run_typer_app
from flink_skill_common.cli_progress import ProgressReporter
from flink_skill_common.migration_manifest import (
    init_or_load_manifest,
    pending_entries,
    read_statement_sql,
    try_load_matching_manifest,
    update_status,
)
from ksql_to_flink.migrate_agent import run_migration
from .ksql_utils import clean_ksql_input, extract_ksql_object_name, split_ksql_create_statements

app = typer.Typer(add_completion=False, no_args_is_help=True)
_HARNESS_DIR = Path(__file__).resolve().parents[2]
_SKILL_PACKAGE_ROOT = _HARNESS_DIR.parent
_PROJECT_ROOT = _SKILL_PACKAGE_ROOT.parent
configure(
    HarnessContext(
        harness_root=_SKILL_PACKAGE_ROOT,
        project_root=_PROJECT_ROOT,
    )
)


def _statement_names(statements: list[str]) -> list[str]:
    """One name per statement (parallel to ``statements``)."""
    names: list[str] = []
    for index, statement in enumerate(statements, start=1):
        cleaned = clean_ksql_input(statement)
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


def _resolve_table_name(
    *,
    entry_table: str,
    entry_name: str,
    table_override: str | None,
    total_statements: int,
) -> str:
    """Per-statement object name; --table only overrides single-statement files."""
    if total_statements == 1 and table_override:
        return table_override
    return entry_table or entry_name


@app.command()
def migrate(
    table: str | None = typer.Option(
        None,
        "--table",
        "-t",
        help="Flink table name override (single-statement files only).",
    ),
    file: Path = typer.Option(..., "--file", "-f"),
    out_dir: Path = typer.Option(Path("output"), "--out-dir", "-o"),
    skip_deploy: bool = typer.Option(False, "--skip-deploy", help="Translate only; do not deploy to CC Flink."),
) -> None:
    """
    Migrate ksqlDB CREATE statements to Flink DDL/DML, one statement at a time.
    """
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
            table=table or "(per-statement)",
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
        statements_dir = None
        manifest = None
        rebuilt = False

        matched = try_load_matching_manifest(file, src_ksql_text)
        if matched is not None:
            manifest, statements_dir = matched
            total = len(manifest.statements)
            if total > 1 and table:
                progress.warn(
                    f"--table={table} ignored for multi-statement file; "
                    "using each CREATE object name as Flink table."
                )
            progress.done(
                2,
                f"Resuming from {statements_dir.name}/manifest.json",
                f"{total} statement(s); split skipped (source sha matches)",
            )
        else:
            progress.step(2, "Splitting ksql statements...")
            ksql_statements = split_ksql_create_statements(src_ksql_text)
            if not ksql_statements:
                typer.echo("No CREATE STREAM/TABLE statements found in file.", err=True)
                raise typer.Exit(1)

            statement_names = _statement_names(ksql_statements)
            total = len(statement_names)
            if total > 1 and table:
                progress.warn(
                    f"--table={table} ignored for multi-statement file; "
                    "using each CREATE object name as Flink table."
                )

            names_summary = ", ".join(statement_names)
            progress.done(
                2,
                f"Found {total} CREATE statement(s)",
                names_summary,
            )

            manifest, statements_dir, rebuilt = init_or_load_manifest(
                file,
                ksql_statements,
                src_ksql_text,
                names=statement_names,
                table_override=table,
                statement_ext=".ksql",
            )
            if rebuilt:
                progress.done(
                    2,
                    f"Wrote statement files to {statements_dir.name}/",
                    f"{len(manifest.statements)} file(s)",
                )

        to_process = pending_entries(manifest)
        skipped = len(manifest.statements) - len(to_process)
        if skipped:
            progress.done(2, f"Skipping {skipped} already migrated statement(s)")

        if not to_process:
            typer.echo(
                f"\nDone. All {len(manifest.statements)} statement(s) already migrated. "
                f"Output: {out_dir.resolve()}"
            )
            return

        if total == 1 and not table:
            only = manifest.statements[0]
            if only.table.startswith("statement_"):
                typer.echo(
                    "Could not extract a table name from the CREATE statement. "
                    "Pass --table explicitly.",
                    err=True,
                )
                raise typer.Exit(1)

        processed = 0
        current_entry = None
        try:
            for entry in to_process:
                current_entry = entry
                ksql_statement = read_statement_sql(statements_dir, entry)
                ksql_cleaned = clean_ksql_input(ksql_statement)
                if not ksql_cleaned.strip():
                    logger.warning("Skipping empty statement index=%d", entry.index)
                    update_status(statements_dir, manifest, entry.index, "migrated")
                    continue

                table_name = _resolve_table_name(
                    entry_table=entry.table,
                    entry_name=entry.name,
                    table_override=table,
                    total_statements=total,
                )
                processed += 1
                remaining_total = len(to_process)
                progress.header(f"[{processed}/{remaining_total}] {entry.name} → {table_name}")
                update_status(statements_dir, manifest, entry.index, "in_progress")

                logger.info(
                    "Migrating statement %d/%d source=%s target=%s ksql=%s",
                    entry.index,
                    total,
                    entry.name,
                    table_name,
                    ksql_cleaned,
                )

                progress.done(1, "Cleaned ksql input", f"{len(ksql_cleaned)} chars")

                progress.step(2, "Running translation agent...")
                response = run_migration(
                    table_name=table_name,
                    ksql=ksql_cleaned,
                    src_ksql=src_ksql_text,
                    source_name=entry.name,
                    on_event=progress.agent_event,
                )
                progress.agent_event(response)
                progress.done(2, "Translation agent finished", f"{len(response)} chars")
                progress.step(3, "Extracting SQL blocks and validating...")
                result = clean_flink_sql_and_validate(
                    response,
                    table_name,
                    ksql_cleaned,
                    skip_deploy,
                    out_dir,
                    on_progress=lambda msg: _on_convergence_progress(progress, msg),
                )
                if result is None:
                    progress.done(3, "Output files written", "no DML")
                    update_status(statements_dir, manifest, entry.index, "migrated")
                elif not result.success:
                    failure_msg = _convergence_failure_message(result)
                    logger.error("Migration failed for table=%s: %s", table_name, failure_msg)
                    progress.done(3, "Validation failed")
                    progress.warn(failure_msg)
                    progress.agent_event(result.ddls[0] if result.ddls else "no DDL")
                    progress.agent_event(result.dmls[0] if result.dmls else "no DML")
                    update_status(
                        statements_dir,
                        manifest,
                        entry.index,
                        "failed",
                        error=failure_msg,
                    )
                    typer.echo(failure_msg, err=True)
                    raise typer.Exit(1)
                else:
                    detail = ""
                    if result.ddl_path is not None:
                        detail = result.ddl_path.name
                    progress.done(3, "Validation finished", detail)
                    progress.agent_event(result.ddls[0] if result.ddls else "no DDL")
                    progress.agent_event(result.dmls[0] if result.dmls else "no DML")
                    update_status(statements_dir, manifest, entry.index, "migrated")
                    if skip_deploy:
                        progress.done(4, "Offline validation passed")
                    else:
                        progress.done(5, "Deploy succeeded")

            typer.echo(
                f"\nDone. Processed {processed} statement(s). Output: {out_dir.resolve()}"
            )
        except KeyboardInterrupt:
            source_name = current_entry.name if current_entry else "unknown"
            if current_entry is not None:
                update_status(
                    statements_dir,
                    manifest,
                    current_entry.index,
                    "interrupted",
                    error="KeyboardInterrupt",
                )
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
