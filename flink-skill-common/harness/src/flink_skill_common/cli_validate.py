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


def _read_sql_files(paths: list[Path]) -> list[str]:
    statements: list[str] = []
    for path in paths:
        if not path.is_file():
            raise typer.BadParameter(f"File not found: {path}")
        statements.append(path.read_text(encoding="utf-8"))
    return statements


def _emit_result(payload: dict[str, Any]) -> None:
    typer.echo(json.dumps(payload, indent=2))
    if not payload.get("ok", False):
        raise typer.Exit(1)


@app.command()
def offline(
    ddl: list[Path] = typer.Option([], "--ddl", help="DDL SQL file(s); repeatable."),
    dml: list[Path] = typer.Option([], "--dml", help="DML SQL file(s); repeatable."),
) -> None:
    """Validate Flink DDL/DML offline using sqlglot (Flink dialect)."""
    load_env()
    ddls = _read_sql_files(ddl)
    dmls = _read_sql_files(dml)
    issues = validate_syntax_for_statements(ddls, dmls)
    _emit_result(_validation_result(issues))


@app.command()
def remote(
    ddl: list[Path] = typer.Option([], "--ddl", help="DDL SQL file(s); repeatable."),
    dml: list[Path] = typer.Option([], "--dml", help="DML SQL file(s); repeatable."),
) -> None:
    """Validate Flink DDL/DML using the Confluent Cloud Flink parser."""
    load_env()
    ddls = _read_sql_files(ddl)
    dmls = _read_sql_files(dml)
    try:
        issues = validate_statements_remote(ddls, dmls)
    except FlinkDeployNotReadyError as exc:
        _emit_result({"ok": False, "error": str(exc)})
        return
    _emit_result(_validation_result(issues))


def main() -> None:
    app()


if __name__ == "__main__":
    main()
