"""Post-migration validation, output writing, and optional deploy."""

from __future__ import annotations

from pathlib import Path

import typer

from flink_skill_common.config import agent_fixer_enabled, get_logger
from flink_skill_common.convergence import ConvergenceContext, converge_flink_sql
from flink_skill_common.output import (
    extract_sql_blocks,
    write_output,
    write_source_ddls,
)

from .ksql_utils import compute_missing_source_tables
from .sources import generate_source_ddls


def _logger():
    return get_logger()


def clean_flink_sql_and_validate(
    response: str,
    table: str,
    src_ksql: str,
    skip_deploy: bool,
    out_dir: Path,
) -> tuple[Path, Path] | None:
    ddls, dmls = extract_sql_blocks(response)
    _logger().info(
        "Extracted ddl=%d statements dml=%d statements",
        len(ddls),
        len(dmls),
    )

    ddl_paths, dml_paths = write_output(table, ddls, dmls, out_dir)
    for path in ddl_paths + dml_paths:
        _logger().info("Wrote %s", path)

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

    if tests_dir is None and (out_dir / "tests").is_dir():
        tests_dir = out_dir / "tests"

    result = converge_flink_sql(
        ddls,
        dmls,
        ConvergenceContext(
            table_name=table,
            source_sql=src_ksql,
            source_label="ksql",
            out_dir=out_dir,
            tests_dir=tests_dir,
        ),
        skip_deploy=skip_deploy,
        agent_on_failure=agent_fixer_enabled(),
    )

    for msg in result.messages:
        typer.echo(msg)

    if result.last_agent_response and not result.success:
        typer.echo(result.last_agent_response)

    if not result.success:
        raise typer.Exit(1)

    if skip_deploy:
        return None

    if result.ddl_path is None:
        typer.echo(f"No DDL file found for table {table!r}", err=True)
        raise typer.Exit(1)

    return result.ddl_path, result.dml_path
