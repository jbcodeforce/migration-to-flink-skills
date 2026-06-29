"""Load references/flink fixtures and run validation helpers for IT tests."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import pytest

from flink_ref_fixtures import FLINK_REF, REPO_ROOT
from flink_skill_common.sql_validate import (
    SqlValidationIssue,
    validate_syntax_for_statements,
    validate_statements_remote,
)

SqlKind = Literal["ddl", "dml"]


def _read_sql_files(directory: Path) -> tuple[list[str], list[str]]:
    ddls = [path.read_text() for path in sorted(directory.glob("ddl*.sql"))]
    dmls = [path.read_text() for path in sorted(directory.glob("dml*.sql"))]
    return ddls, dmls


def load_flink_pair(case: str, *, valid: bool = True) -> tuple[list[str], list[str], Path]:
    """Load DDL/DML lists from references/flink/valid/{case} or invalid/{case}."""
    tier = "valid" if valid else "invalid"
    directory = FLINK_REF / tier / case
    if not directory.is_dir():
        raise FileNotFoundError(f"Fixture case not found: {directory}")
    ddls, dmls = _read_sql_files(directory)
    return (ddls, dmls, directory)


def load_source_sql(case: str) -> str:
    """Load optional source.sql from an invalid fixture case (agent IT context)."""
    path = FLINK_REF / "invalid" / case / "source.sql"
    return path.read_text() if path.is_file() else ""


def validation_issues(
    ddls: list[str],
    dmls: list[str],
    *,
    remote: bool = False,
) -> list[SqlValidationIssue]:
    if remote:
        return validate_statements_remote(ddls, dmls)
    return validate_syntax_for_statements(ddls, dmls)


def assert_no_errors(issues: list[SqlValidationIssue]) -> None:
    errors = [issue for issue in issues if issue.severity == "error"]
    if errors:
        lines = [f"[{e.kind}#{e.statement_index}] {e.message}" for e in errors]
        pytest.fail("Expected no validation errors:\n" + "\n".join(lines))


def assert_has_errors(
    issues: list[SqlValidationIssue],
    *,
    kind: SqlKind | None = None,
) -> None:
    errors = [issue for issue in issues if issue.severity == "error"]
    if kind is not None:
        errors = [issue for issue in errors if issue.kind == kind]
    if not errors:
        pytest.fail(f"Expected validation errors (kind={kind!r}), got: {issues}")


def assert_convergence_stages(
    messages: list[str],
    *,
    expect_offline: bool = True,
    expect_remote: bool = False,
) -> None:
    """Assert converge_flink_sql message trail includes expected validation tiers."""
    joined = "\n".join(messages)
    if expect_offline and "Offline validation failed" not in joined:
        pytest.fail(f"Expected offline validation stage in messages:\n{joined}")
    if expect_remote and "Remote validation failed" not in joined:
        pytest.fail(f"Expected remote validation stage in messages:\n{joined}")
