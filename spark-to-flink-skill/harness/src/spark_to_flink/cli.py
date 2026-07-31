"""CLI entry point for Spark SQL → Flink migration."""

from __future__ import annotations

from pathlib import Path

import typer
from flink_skill_common.agents.factory import resolve_llm_model
from flink_skill_common.cli_interrupt import MIGRATION_INTERRUPT_EXIT_CODE, run_typer_app
from flink_skill_common.cli_progress import ProgressReporter
from flink_skill_common.config import (
    HarnessContext,
    agent_fixer_enabled,
    agent_fixer_max_retries,
    cli_log_file,
    configure,
    get_logger,
    llm_base_url,
    llm_reachable,
    skill_dir,
)
from flink_skill_common.convergence import ConvergenceResult, clean_flink_sql_and_validate
from flink_skill_common.migration_manifest import (
    Manifest,
    StatementEntry,
    init_or_load_manifest,
    pending_entries,
    read_statement_sql,
    try_load_matching_manifest,
    update_status,
)
from flink_skill_common.sql_validate import SqlValidationError
from flink_skill_common.user_errors import format_user_error
from spark_to_flink.migrate_agent import run_migration
from spark_to_flink.sql_utils import (
    clean_sql_input,
    extract_spark_object_name,
    split_sql_create_statements,
)

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
        cleaned = clean_sql_input(statement)
        names.append(extract_spark_object_name(cleaned) or f"statement_{index}")
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


def _warn_if_table_override_ignored(
    progress: ProgressReporter,
    *,
    total: int,
    table: str | None,
) -> None:
    if total > 1 and table:
        progress.warn(
            f"--table={table} ignored for multi-statement file; "
            "using each CREATE object name as Flink table."
        )


def _print_migrate_banner(
    progress: ProgressReporter,
    *,
    table: str | None,
    src_file: Path,
    out_dir: Path,
    skip_deploy: bool,
) -> None:
    progress.banner(
        table=table or "(per-statement)",
        file=str(src_file),
        out_dir=str(out_dir),
        skill=str(skill_dir()),
        deploy="skipped" if skip_deploy else "enabled",
        model=resolve_llm_model(),
        agent_fixer="enabled" if agent_fixer_enabled() else "disabled",
        agent_fixer_retries=str(agent_fixer_max_retries()),
        log=str(cli_log_file()),
    )


def _ensure_src_file(src_file: Path) -> None:
    logger = get_logger()
    if not src_file.exists():
        logger.error("File not found: %s (cwd=%s)", src_file.resolve(), Path.cwd())
        typer.echo(f"File not found: {src_file}", err=True)
        raise typer.Exit(1)


def _ensure_llm_reachable(progress: ProgressReporter) -> None:
    logger = get_logger()
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


def _resume_or_split_manifest(
    progress: ProgressReporter,
    *,
    src_file: Path,
    src_spark_text: str,
    table: str | None,
) -> tuple[Manifest, Path, int]:
    matched = try_load_matching_manifest(src_file, src_spark_text)
    if matched is not None:
        manifest, statements_dir = matched
        total = len(manifest.statements)
        _warn_if_table_override_ignored(progress, total=total, table=table)
        progress.done(
            2,
            f"Resuming from {statements_dir.name}/manifest.json",
            f"{total} statement(s); split skipped (source sha matches)",
        )
        return manifest, statements_dir, total

    progress.step(2, "Splitting Spark SQL statements...")
    spark_statements = split_sql_create_statements(src_spark_text)
    if not spark_statements:
        typer.echo("No CREATE TABLE/VIEW statements found in file.", err=True)
        raise typer.Exit(1)

    statement_names = _statement_names(spark_statements)
    total = len(statement_names)
    _warn_if_table_override_ignored(progress, total=total, table=table)
    progress.done(
        2,
        f"Found {total} CREATE statement(s)",
        ", ".join(statement_names),
    )

    manifest, statements_dir, rebuilt = init_or_load_manifest(
        src_file,
        spark_statements,
        src_spark_text,
        names=statement_names,
        table_override=table,
        statement_ext=".sql",
    )
    if rebuilt:
        progress.done(
            2,
            f"Wrote statement files to {statements_dir.name}/",
            f"{len(manifest.statements)} file(s)",
        )
    return manifest, statements_dir, total


def _pending_or_already_done(
    progress: ProgressReporter,
    *,
    manifest: Manifest,
    out_dir: Path,
) -> list[StatementEntry] | None:
    to_process = pending_entries(manifest)
    skipped = len(manifest.statements) - len(to_process)
    if skipped:
        progress.done(2, f"Skipping {skipped} already migrated statement(s)")
    if not to_process:
        typer.echo(
            f"\nDone. All {len(manifest.statements)} statement(s) already migrated. "
            f"Output: {out_dir.resolve()}"
        )
        return None
    return to_process


def _ensure_table_name_resolvable(
    *,
    manifest: Manifest,
    total: int,
    table: str | None,
) -> None:
    if total == 1 and not table:
        only = manifest.statements[0]
        if only.table.startswith("statement_"):
            typer.echo(
                "Could not extract a table name from the CREATE statement. "
                "Pass --table explicitly.",
                err=True,
            )
            raise typer.Exit(1)


def _apply_convergence_result(
    progress: ProgressReporter,
    *,
    result: ConvergenceResult | None,
    table_name: str,
    statements_dir: Path,
    manifest: Manifest,
    entry: StatementEntry,
    skip_deploy: bool,
) -> None:
    logger = get_logger()
    if result is None:
        progress.done(3, "Output files written", "no DML")
        update_status(statements_dir, manifest, entry.index, "migrated")
        return

    if not result.success:
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

    detail = result.ddl_path.name if result.ddl_path is not None else ""
    progress.done(3, "Validation finished", detail)
    progress.agent_event(result.ddls[0] if result.ddls else "no DDL")
    progress.agent_event(result.dmls[0] if result.dmls else "no DML")
    update_status(statements_dir, manifest, entry.index, "migrated")
    if skip_deploy:
        progress.done(4, "Offline validation passed")
    else:
        progress.done(5, "Deploy succeeded")


def _migrate_one_statement(
    progress: ProgressReporter,
    *,
    entry: StatementEntry,
    statements_dir: Path,
    manifest: Manifest,
    src_spark_text: str,
    src_file: Path,
    table: str | None,
    total: int,
    processed: int,
    remaining_total: int,
    skip_deploy: bool,
    out_dir: Path,
) -> int:
    """Migrate one pending statement. Returns updated processed count."""
    logger = get_logger()
    spark_statement = read_statement_sql(statements_dir, entry)
    spark_cleaned = clean_sql_input(spark_statement)
    if not spark_cleaned.strip():
        logger.warning("Skipping empty statement index=%d", entry.index)
        update_status(statements_dir, manifest, entry.index, "migrated")
        return processed

    table_name = _resolve_table_name(
        entry_table=entry.table,
        entry_name=entry.name,
        table_override=table,
        total_statements=total,
    )
    processed += 1
    progress.header(f"[{processed}/{remaining_total}] {entry.name} → {table_name}")
    update_status(statements_dir, manifest, entry.index, "in_progress")

    logger.info(
        "Migrating statement %d/%d source=%s target=%s spark=%s",
        entry.index,
        total,
        entry.name,
        table_name,
        spark_cleaned,
    )

    progress.done(1, "Cleaned Spark SQL input", f"{len(spark_cleaned)} chars")

    progress.step(2, "Running translation agent...")
    response = run_migration(
        table_name=table_name,
        spark_sql=spark_cleaned,
        src_spark=src_spark_text,
        source_name=entry.name,
        src_file=src_file,
        on_event=progress.agent_event,
    )
    progress.agent_event(response)
    progress.done(2, "Translation agent finished", f"{len(response)} chars")

    progress.step(3, "Extracting SQL blocks and validating...")
    result = clean_flink_sql_and_validate(
        response,
        table_name,
        spark_cleaned,
        skip_deploy,
        out_dir,
        on_progress=lambda msg: _on_convergence_progress(progress, msg),
    )
    _apply_convergence_result(
        progress,
        result=result,
        table_name=table_name,
        statements_dir=statements_dir,
        manifest=manifest,
        entry=entry,
        skip_deploy=skip_deploy,
    )
    return processed


def _handle_migration_interrupt(
    *,
    current_entry: StatementEntry | None,
    statements_dir: Path,
    manifest: Manifest,
    processed: int,
    total: int,
) -> None:
    logger = get_logger()
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


def _process_pending_statements(
    progress: ProgressReporter,
    *,
    to_process: list[StatementEntry],
    statements_dir: Path,
    manifest: Manifest,
    src_spark_text: str,
    src_file: Path,
    table: str | None,
    total: int,
    skip_deploy: bool,
    out_dir: Path,
) -> None:
    """Process pending statements."""
    logger = get_logger()
    logger.info("process_pending_statements start to_process=%d", len(to_process))
    processed = 0
    current_entry: StatementEntry | None = None
    try:
        for entry in to_process:
            current_entry = entry
            processed = _migrate_one_statement(
                progress,
                entry=entry,
                statements_dir=statements_dir,
                manifest=manifest,
                src_spark_text=src_spark_text,
                src_file=src_file,
                table=table,
                total=total,
                processed=processed,
                remaining_total=len(to_process),
                skip_deploy=skip_deploy,
                out_dir=out_dir,
            )
        typer.echo(
            f"\nDone. Processed {processed} statement(s). Output: {out_dir.resolve()}"
        )
    except KeyboardInterrupt:
        _handle_migration_interrupt(
            current_entry=current_entry,
            statements_dir=statements_dir,
            manifest=manifest,
            processed=processed,
            total=total,
        )


@app.command()
def migrate(
    table: str | None = typer.Option(
        None,
        "--table",
        "-t",
        help="Flink table name override (single-statement files only).",
    ),
    src_file: Path = typer.Option(..., "--file", "-f"),
    out_dir: Path = typer.Option(Path("output"), "--out-dir", "-o"),
    skip_deploy: bool = typer.Option(
        False, "--skip-deploy", help="Translate only; do not deploy to CC Flink."
    ),
) -> None:
    """Migrate Spark SQL CREATE statements to Flink DDL/DML, one statement at a time."""
    logger = get_logger()
    logger.info("skill_dir=%s", skill_dir())
    progress = ProgressReporter()
    logger.info(
        "migrate start table=%s file=%s out_dir=%s skip_deploy=%s",
        table,
        src_file,
        out_dir,
        skip_deploy,
    )
    try:
        _print_migrate_banner(
            progress,
            table=table,
            src_file=src_file,
            out_dir=out_dir,
            skip_deploy=skip_deploy,
        )
        _ensure_src_file(src_file)
        _ensure_llm_reachable(progress)

        src_spark_text = src_file.read_text()
        manifest, statements_dir, total = _resume_or_split_manifest(
            progress,
            src_file=src_file,
            src_spark_text=src_spark_text,
            table=table,
        )
        to_process = _pending_or_already_done(
            progress,
            manifest=manifest,
            out_dir=out_dir,
        )
        if to_process is None:
            return

        _ensure_table_name_resolvable(manifest=manifest, total=total, table=table)
        _process_pending_statements(
            progress,
            to_process=to_process,
            statements_dir=statements_dir,
            manifest=manifest,
            src_spark_text=src_spark_text,
            src_file=src_file,
            table=table,
            total=total,
            skip_deploy=skip_deploy,
            out_dir=out_dir,
        )

    except typer.Exit:
        raise
    except SqlValidationError as exc:
        logger.error("SQL validation failed: %s", exc)
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    except Exception as exc:
        logger.exception("migrate failed table=%s file=%s", table, src_file)
        typer.echo(f"Error: {format_user_error(exc)}", err=True)
        raise typer.Exit(1) from exc


def main() -> None:
    get_logger()
    run_typer_app(app)


if __name__ == "__main__":
    main()
