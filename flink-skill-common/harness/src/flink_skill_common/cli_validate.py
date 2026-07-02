"""CLI for offline and remote Flink SQL validation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import typer

from flink_skill_common.config import FlinkDeployNotReadyError, HarnessContext, configure, load_env
from flink_skill_common.sql_validate import (
    SqlValidationIssue,
    validate_statements_remote,
    validate_syntax_for_statements,
)
from flink_skill_common.sql_parse import is_create_table_statement, is_insert_into_statement
from flink_skill_common.convergence import ConvergenceContext, converge_flink_sql


_HARNESS_ROOT = Path(__file__).resolve().parents[2]
_PROJECT_ROOT = _HARNESS_ROOT.parent
configure(HarnessContext(harness_root=_HARNESS_ROOT, project_root=_PROJECT_ROOT))

app = typer.Typer(add_completion=False, no_args_is_help=True)


def _issue_dict(issue: SqlValidationIssue) -> dict[str, Any]:
    return {
        "statement_index": issue.statement_index,
        "kind": issue.kind,
        "message": issue.message,
        "line": issue.line,
        "severity": issue.severity,
    }


def _validation_result(issues: list[SqlValidationIssue]) -> dict[str, Any]:
    errors = [issue for issue in issues if issue.severity == "error"]
    return {
        "ok": not errors,
        "issues": [_issue_dict(issue) for issue in issues],
        "error_count": len(errors),
    }

def _classify_fixture(path: Path, sql: str) -> str:
    """Classify fixture SQL as ddl or dml; content wins over filename prefix."""
    stripped = sql.strip()
    if is_create_table_statement(stripped):
        return "ddl"
    if is_insert_into_statement(stripped):
        return "dml"

    name = path.name
    if name.startswith("ddl"):
        return "ddl"
    if name.startswith("dml") or name.startswith("insert"):
        return "dml"
    raise ValueError(f"Unrecognized fixture SQL file: {path}")

def read_sql_files(directory: Path) -> tuple[list[str], list[str]]:
    ddls: list[str] = []
    dmls: list[str] = []
    seen: set[Path] = set()
    for pattern in ("**/ddl*.sql", "**/dml*.sql", "**/tests/*.sql"):
        for path in sorted(directory.glob(pattern)):
            if path in seen:
                continue
            seen.add(path)
            sql = path.read_text()
            if _classify_fixture(path, sql) == "ddl":
                ddls.append(sql)
            else:
                dmls.append(sql)
    return ddls, dmls

def _emit_result(payload: dict[str, Any]) -> None:
    typer.echo(json.dumps(payload, indent=2))
    if not payload.get("ok", False):
        raise typer.Exit(1)

def _read_sql_file_from_paths(paths: list[Path]) -> list[str]:
    statements: list[str] = []
    for path in paths:
        if not path.is_file():
            raise typer.BadParameter(f"File not found: {path}")
        statements.append(path.read_text(encoding="utf-8"))
    return statements


@app.command()
def syntax_only(
    ddl: list[Path] = typer.Option([], "--ddl", help="DDL SQL file(s); repeatable."),
    dml: list[Path] = typer.Option([], "--dml", help="DML SQL file(s); repeatable."),
) -> None:
    """Validate Flink DDL/DML offline using sqlglot (Flink dialect)."""
    load_env()
    ddls = _read_sql_file_from_paths(ddl)
    dmls = _read_sql_file_from_paths(dml)
    issues = validate_syntax_for_statements(ddls, dmls)
    _emit_result(_validation_result(issues))


@app.command()
def remote(
    ddl: list[Path] = typer.Option([], "--ddl", help="DDL SQL file(s); repeatable."),
    dml: list[Path] = typer.Option([], "--dml", help="DML SQL file(s); repeatable."),
) -> None:
    """Validate Flink DDL/DML using the Confluent Cloud Flink parser."""
    load_env()
    ddls = _read_sql_file_from_paths(ddl)
    dmls = _read_sql_file_from_paths(dml)
    try:
        issues = validate_syntax_for_statements(ddls, dmls)
        if issues:
            _emit_result({"ok": False, "error": "Syntax errors found"})
            for issue in issues:
                 _emit_result({"ok": False, "error": issue.message})
            return
        else:
            _emit_result({"ok": True, "error": "No syntax errors found...continuing with validation of statements."})
        issues = validate_statements_remote(ddls, dmls)
    except FlinkDeployNotReadyError as exc:
        _emit_result({"ok": False, "error": str(exc)})
        return
    _emit_result(_validation_result(issues))

@app.command()
def validate_flink_sqls(
    table_name: str = typer.Option(...),
    flink_sql_dir: Path = typer.Option(...),
    target_dir: Path = typer.Option(...),
) -> None:
    """Validate Flink DDL/DML using the Confluent Cloud Flink and Agent Fixer."""
    load_env()
    if not flink_sql_dir.is_dir():
        raise FileNotFoundError(f"Fixture case not found: {flink_sql_dir}")
    ddls, dmls = read_sql_files(flink_sql_dir)

    ctx = ConvergenceContext(
        table_name=table_name,
        source_sql=dmls[0],
        source_label="fixture",
        out_dir=target_dir,
        tests_dir=flink_sql_dir / "tests",
    )
    result = converge_flink_sql(ddls, dmls, ctx, skip_deploy=False, agent_on_failure=True)    
    print(result)

def main() -> None:
    app()


if __name__ == "__main__":
    main()
