"""Shared helpers for references/flink SQL fixtures (UT + IT)."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import pytest

from flink_skill_common.sql_parse import is_create_table_statement, is_insert_into_statement
from flink_skill_common.sql_validate import (
    SqlValidationIssue,
    validate_syntax_for_statements,
    validate_statements_remote,
)

from flink_skill_common.cli_validate import read_sql_files

REPO_ROOT = Path(__file__).resolve().parents[3]
FLINK_REF = REPO_ROOT / "references" / "flink"
FLINK_VALID_REF = FLINK_REF / "valid"
REFERENCES_ROOT = REPO_ROOT / "references"

SqlKind = Literal["ddl", "dml"]


def load_flink_pair(directory: Path) -> tuple[list[str], list[str], Path]:
    """Load DDL/DML lists from a fixture case directory (includes tests/*.sql)."""
    if not directory.is_dir():
        raise FileNotFoundError(f"Fixture case not found: {directory}")
    ddls, dmls = read_sql_files(directory)
    return (ddls, dmls, directory)


def load_all_valid_flink_reference_sql() -> tuple[list[str], list[str]]:
    """Load every *.sql under references/flink/valid, split into DDL and DML lists."""
    if not FLINK_VALID_REF.is_dir():
        raise FileNotFoundError(f"Missing fixture root: {FLINK_VALID_REF}")
    ddls, dmls = read_sql_files(FLINK_VALID_REF)
    return (ddls, dmls)


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
