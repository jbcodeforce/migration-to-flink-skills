"""Offline and remote Flink SQL syntax validation."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from flink_skill_common.logging_config import get_logger
from flink_skill_common.sql_preprocess import strip_sql_comments_and_drops

_CREATE_TABLE_START = re.compile(r"^\s*CREATE\s+TABLE\b", re.IGNORECASE)
_INSERT_INTO_START = re.compile(r"^\s*INSERT\s+INTO\b", re.IGNORECASE)
_FLINK_DDL_EXTENSIONS = re.compile(r"\)\s*(DISTRIBUTED\s+BY|WITH\s*\()", re.IGNORECASE)
_CONNECTOR_PATTERN = re.compile(r"'connector'\s*=", re.IGNORECASE)

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
        ast = sqlglot.parse_one(parse_sql, read="spark")
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
        if _FLINK_DDL_EXTENSIONS.search(stripped) and not _CONNECTOR_PATTERN.search(stripped):
            issues.append(
                SqlValidationIssue(
                    statement_index=index,
                    kind=kind,
                    message="DDL WITH clause missing 'connector' property",
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


def validate_statements(ddls: list[str], dmls: list[str]) -> list[SqlValidationIssue]:
    """Tier 1: offline syntax validation via sqlglot (Spark dialect)."""
    issues: list[SqlValidationIssue] = []
    for index, sql in enumerate(ddls):
        issues.extend(_validate_one(sql, "ddl", index))
    for index, sql in enumerate(dmls):
        issues.extend(_validate_one(sql, "dml", index))
    return issues


def log_validation_issues(issues: list[SqlValidationIssue]) -> None:
    """Log warnings; errors are raised by raise_on_errors."""
    logger = get_logger()
    for issue in issues:
        if issue.severity == "warning":
            logger.warning(
                "SQL validation warning [%s#%d]: %s",
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
    from flink_skill_common.config import require_flink_deploy_ready
    from flink_skill_common.deploy.flink_statement_manager import FlinkStatementManager

    require_flink_deploy_ready()
    return FlinkStatementManager().validate_statements(ddls, dmls)
