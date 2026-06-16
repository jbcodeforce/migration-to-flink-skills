"""Deploy DDL/DML Flink statements via confluent-sql."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

from flink_skill_common.deploy.flink_statement_manager import (
    FAILURE_PHASES,
    SUCCESS_PHASES,
    FlinkStatementManager,
    StatementManagerError,
)

STATEMENT_NAME_RE = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")


class DeployError(RuntimeError):
    """Flink statement deploy failed."""


@dataclass
class StatementDeployStatus:
    name: str
    phase: str
    raw: str = ""


@dataclass
class DeployResult:
    table_name: str
    ddl_statement: str
    dml_statement: str
    ddl_phase: str
    dml_phase: str
    health_status: str = ""
    exceptions: str = ""
    success: bool = True
    messages: list[str] = field(default_factory=list)
    source_statements: list[tuple[str, str]] = field(default_factory=list)


def normalize_statement_prefix(table_name: str) -> str:
    """Normalize table name for Flink statement names (hyphens, lowercase)."""
    normalized = table_name.lower().replace("_", "-")
    if not STATEMENT_NAME_RE.match(normalized):
        raise DeployError(
            f"Table name {table_name!r} cannot be normalized to a valid statement name prefix"
        )
    return normalized


def ddl_statement_name(table_name: str) -> str:
    return f"{normalize_statement_prefix(table_name)}-ddl"


def dml_statement_name(table_name: str) -> str:
    return f"{normalize_statement_prefix(table_name)}-dml"


def discover_source_ddl_files(tests_dir: Path) -> list[tuple[str, Path]]:
    """Return (table_name, path) for each tests/ddl.{table}.sql source stub."""
    if not tests_dir.is_dir():
        return []
    results: list[tuple[str, Path]] = []
    for path in sorted(tests_dir.glob("ddl.*.sql")):
        stem = path.stem
        if stem.startswith("ddl."):
            table_name = stem[4:]
            if table_name:
                results.append((table_name, path))
    return results


def _poll_statement_phase(
    manager: FlinkStatementManager,
    statement_name: str,
) -> StatementDeployStatus:
    try:
        result = manager.wait_for_phase(statement_name, SUCCESS_PHASES | FAILURE_PHASES)
    except StatementManagerError as exc:
        raise DeployError(str(exc)) from exc
    return StatementDeployStatus(
        name=statement_name,
        phase=str(result.get("phase", "UNKNOWN")),
        raw=json.dumps(result),
    )


def _deploy_source_ddls(
    manager: FlinkStatementManager,
    tests_dir: Path | None,
    messages: list[str],
) -> list[tuple[str, str]]:
    """Deploy source stub DDLs from tests/ddl.*.sql before target statements."""
    source_statements: list[tuple[str, str]] = []
    if tests_dir is None:
        return source_statements

    for source_table, source_path in discover_source_ddl_files(tests_dir):
        source_sql = source_path.read_text().strip()
        if not source_sql:
            continue
        source_name = ddl_statement_name(source_table)
        try:
            create_result = manager.create_statement(source_name, source_sql)
        except StatementManagerError as exc:
            raise DeployError(f"create-flink-statement failed for {source_name}: {exc}") from exc

        phase = str(create_result.get("phase", "PENDING"))
        messages.append(f"Created source DDL statement {source_name}")

        status = _poll_statement_phase(manager, source_name)
        phase = status.phase
        messages.append(f"Source DDL {source_name} phase: {phase}")
        source_statements.append((source_name, phase))

        if phase in FAILURE_PHASES:
            exceptions = manager.get_statement_exceptions(source_name)
            raise DeployError(
                f"Source DDL {source_name} failed with phase {phase}: {exceptions}"
            )

    return source_statements


def _deploy_table(
    manager: FlinkStatementManager,
    table_name: str,
    ddl_path: Path,
    dml_path: Path,
    tests_dir: Path | None = None,
) -> DeployResult:
    ddl_sql = ddl_path.read_text().strip()
    dml_sql = dml_path.read_text().strip() if dml_path.is_file() else ""

    ddl_name = ddl_statement_name(table_name)
    dml_name = dml_statement_name(table_name)
    messages: list[str] = []

    source_statements = _deploy_source_ddls(manager, tests_dir, messages)

    if not ddl_sql:
        raise DeployError(f"DDL file is empty: {ddl_path}")

    try:
        manager.create_statement(ddl_name, ddl_sql)
    except StatementManagerError as exc:
        raise DeployError(f"create-flink-statement failed for {ddl_name}: {exc}") from exc
    messages.append(f"Created DDL statement {ddl_name}")

    ddl_status = _poll_statement_phase(manager, ddl_name)
    ddl_phase = ddl_status.phase
    messages.append(f"DDL {ddl_name} phase: {ddl_phase}")

    if ddl_phase in FAILURE_PHASES:
        exceptions = manager.get_statement_exceptions(ddl_name)
        raise DeployError(f"DDL {ddl_name} failed with phase {ddl_phase}: {exceptions}")

    dml_phase = ""
    health = ""
    exceptions = ""

    if dml_sql:
        try:
            manager.create_statement(dml_name, dml_sql)
        except StatementManagerError as exc:
            raise DeployError(f"create-flink-statement failed for {dml_name}: {exc}") from exc
        messages.append(f"Created DML statement {dml_name}")

        dml_status = _poll_statement_phase(manager, dml_name)
        dml_phase = dml_status.phase
        messages.append(f"DML {dml_name} phase: {dml_phase}")

        if dml_phase in FAILURE_PHASES:
            exceptions = json.dumps(manager.get_statement_exceptions(dml_name))
            raise DeployError(f"DML {dml_name} failed with phase {dml_phase}: {exceptions}")

        health_result = manager.check_statement_health(dml_name)
        health = json.dumps(health_result)
        messages.append(f"Health: {health[:200]}")

    success = ddl_phase in SUCCESS_PHASES and (
        not dml_sql or dml_phase in SUCCESS_PHASES
    )

    return DeployResult(
        table_name=table_name,
        ddl_statement=ddl_name,
        dml_statement=dml_name if dml_sql else "",
        ddl_phase=ddl_phase,
        dml_phase=dml_phase,
        health_status=health,
        exceptions=exceptions,
        success=success,
        messages=messages,
        source_statements=source_statements,
    )


def deploy_table(
    table_name: str,
    ddl_path: Path,
    dml_path: Path,
    tests_dir: Path | None = None,
    *,
    manager: FlinkStatementManager | None = None,
) -> DeployResult:
    """Deploy source DDLs (tests/), target DDL, then DML to Confluent Cloud Flink."""
    mgr = manager or FlinkStatementManager()
    return _deploy_table(mgr, table_name, ddl_path, dml_path, tests_dir)
