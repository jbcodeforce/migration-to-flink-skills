"""
Copyright 2024-2026 Confluent, Inc.
KSQL to Flink SQL Translation Agent

Offline and remote Flink SQL syntax validation.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from flink_skill_common.config import get_logger
from flink_skill_common.sql_preprocess import strip_sql_comments_and_drops, compute_missing_source_tables
from flink_skill_common.sqlglot_flink import Flink  # noqa: F401 — registers read="flink"
from flink_skill_common.output import (
    extract_sql_blocks,
    write_output,
    write_source_ddls,
)
from flink_skill_common.agents.sources import generate_source_ddls
from flink_skill_common.config import agent_fixer_enabled

_CREATE_TABLE_START = re.compile(r"^\s*CREATE\s+TABLE\b", re.IGNORECASE)
_INSERT_INTO_START = re.compile(r"^\s*INSERT\s+INTO\b", re.IGNORECASE)
_FLINK_DDL_EXTENSIONS = re.compile(r"\)\s*(DISTRIBUTED\s+BY|WITH\s*\()", re.IGNORECASE)
_CHANGELOG_PATTERN = re.compile(r"'changelog.mode'\s*=", re.IGNORECASE)

SqlKind = Literal["ddl", "dml"]
Severity = Literal["error", "warning"]


@dataclass(frozen=True)
class SqlValidationIssue:
    statement_index: int
    kind: SqlKind
    message: str
    line: int | None = None
    severity: Severity = "error"


class SqlValidationError(RuntimeError):
    """One or more SQL statements failed validation."""

    def __init__(self, issues: list[SqlValidationIssue]) -> None:
        self.issues = [i for i in issues if i.severity == "error"]
        lines = [
            f"[{issue.kind}#{issue.statement_index}] {issue.message}"
            + (f" (line {issue.line})" if issue.line else "")
            for issue in self.issues
        ]
        super().__init__("SQL validation failed:\n" + "\n".join(lines))


def _extract_ddl_header(sql: str) -> str:
    """Return CREATE TABLE (...) portion when Flink DDL extensions follow."""
    match = _FLINK_DDL_EXTENSIONS.search(sql)
    if match:
        return sql[: match.start() + 1].strip()
    semi = sql.rfind(";")
    if semi != -1:
        return sql[: semi + 1].strip()
    return sql.strip()


def _parse_error_message(exc: ParseError) -> tuple[str, int | None]:
    if exc.errors:
        err = exc.errors[0]
        description = err.get("description", str(exc))
        line = err.get("line")
        return str(description), int(line) if line is not None else None
    return str(exc), None


def _validate_parseable(sql: str, kind: SqlKind, index: int) -> list[SqlValidationIssue]:
    issues: list[SqlValidationIssue] = []
    parse_sql = _extract_ddl_header(sql) if kind == "ddl" else sql

    try:
        ast = sqlglot.parse_one(parse_sql, read="flink")
    except ParseError as exc:
        message, line = _parse_error_message(exc)
        issues.append(
            SqlValidationIssue(
                statement_index=index,
                kind=kind,
                message=message,
                line=line,
                severity="error",
            )
        )
        return issues

    if kind == "ddl" and isinstance(ast, exp.Command):
        issues.append(
            SqlValidationIssue(
                statement_index=index,
                kind=kind,
                message="Flink-specific DDL tail parsed as Command; offline check is partial",
                severity="warning",
            )
        )
    return issues


def _validate_one(sql: str, kind: SqlKind, index: int) -> list[SqlValidationIssue]:
    stripped = strip_sql_comments_and_drops(sql).strip()
    print(f"Stripped: {stripped[:60]}...")
    if not stripped:
        return []

    issues: list[SqlValidationIssue] = []
    if kind == "ddl":
        if not _CREATE_TABLE_START.match(stripped):
            return [
                SqlValidationIssue(
                    statement_index=index,
                    kind=kind,
                    message="DDL must start with CREATE TABLE",
                    severity="error",
                )
            ]
        if _FLINK_DDL_EXTENSIONS.search(stripped) and not _CHANGELOG_PATTERN.search(stripped):
            issues.append(
                SqlValidationIssue(
                    statement_index=index,
                    kind=kind,
                    message="DDL WITH clause missing 'changelog.mode' property",
                    severity="warning",
                )
            )
    elif not _INSERT_INTO_START.match(stripped):
        return [
            SqlValidationIssue(
                statement_index=index,
                kind=kind,
                message="DML must start with INSERT INTO",
                severity="error",
            )
        ]

    issues.extend(_validate_parseable(stripped, kind, index))
    return issues


def validate_syntax_for_statements(ddls: list[str], dmls: list[str]) -> list[SqlValidationIssue]:
    """Tier 1: offline syntax validation via sqlglot (Flink dialect)."""
    issues: list[SqlValidationIssue] = []
    for index, sql in enumerate(ddls):
        issues.extend(_validate_one(sql, "ddl", index))
    for index, sql in enumerate(dmls):
        issues.extend(_validate_one(sql, "dml", index))
    return issues


def log_validation_issues(issues: list[SqlValidationIssue]) -> None:
    """Log warnings"""
    logger = get_logger()
    for issue in issues:
        if issue.severity == "warning":
            logger.warning(
                "SQL validation warning [%s#%d]: %s",
                issue.kind,
                issue.statement_index,
                issue.message,
            )
        else:
            logger.error(
                "SQL validation error [%s#%d]: %s",
                issue.kind,
                issue.statement_index,
                issue.message,
            )

def raise_on_errors(issues: list[SqlValidationIssue]) -> None:
    """Raise SqlValidationError when any error-severity issue is present."""
    errors = [issue for issue in issues if issue.severity == "error"]
    if errors:
        raise SqlValidationError(errors)
        
def validate_statements_remote(ddls: list[str], dmls: list[str]) -> list[SqlValidationIssue]:
    """Tier 2: authoritative validation via Confluent Cloud Flink parser."""
    from flink_skill_common.config import flink_deploy_settings
    from flink_skill_common.deploy.flink_statement_manager import FlinkStatementManager

    flk_settings= flink_deploy_settings()
    return FlinkStatementManager(flk_settings).validate_statements(ddls, dmls)



def clean_flink_sql_and_validate(
    response: str,
    table: str,
    src_ksql: str,
    skip_deploy: bool,
    out_dir: Path,
    *,
    on_progress: Callable[[str], None] | None = None,
):
    """
    From the LLM response, extract DDL/DML, write output, and run convergence.
    Returns ConvergenceResult when DML is present, otherwise None.
    """
    from flink_skill_common.convergence import ConvergenceContext, converge_flink_sql

    def _emit(message: str) -> None:
        if on_progress is not None:
            on_progress(message)

    ddls, dmls = extract_sql_blocks(response)
    _emit(f"Extracted {len(ddls)} DDL, {len(dmls)} DML")
    table_dir = out_dir / table
    table_dir.mkdir(parents=True, exist_ok=True)
    tests_dir = table_dir / "tests"
    tests_dir.mkdir(parents=True, exist_ok=True)
    _emit(f"Writing output files to {table_dir}")
    write_output(table, ddls, dmls, table_dir)

    if not dmls:
        _emit("No DML in response; skipped validation and deploy")
        return None

    dml_sql = "\n\n".join(dmls)
    ddl_sql = "\n\n".join(ddls)
    missing = compute_missing_source_tables(dml_sql, table, ddl_sql)
    if missing:
        _emit(f"Missing source tables: {', '.join(missing)}")
        _emit("Generating source DDL stubs...")
        source_ddls = generate_source_ddls(table, src_ksql, dml_sql, missing)
        write_source_ddls(table_dir, source_ddls)
        _emit(f"Wrote {len(source_ddls)} source DDL stub(s)")

    return converge_flink_sql(
        ddls,
        dmls,
        ConvergenceContext(
            table_name=table,
            source_sql=src_ksql,
            source_label="ksql",
            out_dir=table_dir,
            tests_dir=tests_dir,
        ),
        skip_deploy=skip_deploy,
        agent_on_failure=agent_fixer_enabled(),
        on_progress=on_progress,
    )