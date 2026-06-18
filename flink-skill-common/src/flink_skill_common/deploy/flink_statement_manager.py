"""Flink statement lifecycle via confluent-sql REST driver."""

from __future__ import annotations

import json
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Literal

import confluent_sql
from confluent_sql.exceptions import OperationalError, StatementNotFoundError

from flink_skill_common.config import FlinkDeploySettings, flink_deploy_settings
from flink_skill_common.deploy.statements import (
    ddl_statement_name,
    discover_source_ddl_files,
    dml_statement_name,
)

from logging import getLogger

logger = getLogger(__name__)

SqlKind = Literal["snapshot_ddl", "streaming_dml", "batch_dml", "streaming_ddl"]

SUCCESS_PHASES = frozenset({"RUNNING", "COMPLETED", "APPLIED", "STOPPED", "DELETED"})
FAILURE_PHASES = frozenset({"FAILED", "FAILING"})


class StatementManagerError(RuntimeError):
    """Flink statement operation failed."""


class DeployError(RuntimeError):
    """Flink table deploy failed."""


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


def classify_sql(sql: str) -> SqlKind:
    """Classify SQL for the correct confluent-sql execution path."""
    s = sql.strip().lower()
    while s.startswith("--"):
        nl = s.find("\n")
        if nl == -1:
            return "snapshot_ddl"
        s = s[nl + 1 :].lstrip()

    if s.startswith("insert into"):
        if " select " in s:
            return "streaming_dml"
        return "batch_dml"
    if s.startswith("create table ") and " as select " in s:
        return "streaming_ddl"
    if s.startswith(("create table ", "drop table ")):
        return "snapshot_ddl"
    return "snapshot_ddl"


def _statement_phase(stmt: Any) -> str:
    status = getattr(stmt, "status", None)
    if isinstance(status, dict):
        return str(status.get("phase", "UNKNOWN"))
    return "UNKNOWN"


def _statement_detail(stmt: Any) -> str:
    status = getattr(stmt, "status", None)
    if isinstance(status, dict):
        return str(status.get("detail", ""))
    return ""


class FlinkStatementManager:
    """Manage Confluent Cloud Flink SQL statements via confluent-sql."""

    def __init__(self, settings: FlinkDeploySettings | None = None) -> None:
        self._settings = settings or flink_deploy_settings()

    @property
    def settings(self) -> FlinkDeploySettings:
        return self._settings

    @contextmanager
    def connect(self) -> Iterator[Any]:
        """Open a confluent-sql connection."""
        connect_kwargs: dict[str, Any] = {
            "flink_api_key": self._settings.flink_api_key,
            "flink_api_secret": self._settings.flink_api_secret,
            "environment_id": self._settings.environment_id,
            "compute_pool_id": self._settings.compute_pool_id,
            "organization_id": self._settings.organization_id,
            "database": self._settings.database_name,
            "http_user_agent": self._settings.http_user_agent,
        }
        if self._settings.endpoint:
            connect_kwargs["endpoint"] = self._settings.endpoint
        else:
            connect_kwargs["cloud_provider"] = self._settings.cloud_provider
            connect_kwargs["cloud_region"] = self._settings.cloud_region

        conn = confluent_sql.connect(**connect_kwargs)
        try:
            yield conn
        finally:
            conn.close()

    def get_statement(self, statement_name: str) -> dict[str, Any]:
        """Return normalized statement status."""
        with self.connect() as conn:
            try:
                stmt = conn.get_statement(statement_name)
            except StatementNotFoundError:
                return {
                    "name": statement_name,
                    "phase": "NOT_FOUND",
                    "detail": "Statement not found",
                }
            return {
                "name": statement_name,
                "phase": _statement_phase(stmt),
                "detail": _statement_detail(stmt),
            }

    def list_statements(self, page_size: int = 50) -> dict[str, Any]:
        """List Flink statements (first REST page)."""
        with self.connect() as conn:
            resp = conn._request(  # noqa: SLF001
                "/statements",
                params={"page_size": page_size},
            )
            payload = resp.json() if hasattr(resp, "json") else {}
            items = payload.get("data") if isinstance(payload, dict) else payload
            if not isinstance(items, list):
                items = []
            normalized = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                status = item.get("status") or {}
                normalized.append(
                    {
                        "name": item.get("name") or item.get("statementName") or "",
                        "phase": status.get("phase", "UNKNOWN"),
                        "detail": status.get("detail", ""),
                    }
                )
            return {"statements": normalized, "count": len(normalized)}

    def delete_statement(self, statement_name: str) -> dict[str, Any]:
        """Delete a statement and wait until it is gone."""
        with self.connect() as conn:
            try:
                conn.delete_statement(statement_name)
            except StatementNotFoundError:
                return {"name": statement_name, "status": "not_found"}

            deadline = time.monotonic() + self._settings.timeout_seconds
            poll = self._settings.poll_seconds
            while time.monotonic() < deadline:
                try:
                    conn.get_statement(statement_name)
                except StatementNotFoundError:
                    return {"name": statement_name, "status": "deleted"}
                time.sleep(poll)

            raise StatementManagerError(
                f"Statement {statement_name} still present after delete timeout"
            )

    def _submit_on_connection(
        self,
        conn: Any,
        statement_name: str,
        sql: str,
    ) -> dict[str, Any]:
        kind = classify_sql(sql)
        pool = self._settings.compute_pool_id
        timeout = int(self._settings.timeout_seconds)
        props: dict[str, str] = {}

        if kind == "snapshot_ddl":
            stmt = conn.execute_snapshot_ddl(
                sql,
                statement_name=statement_name,
                properties=props,
                compute_pool_id=pool,
                timeout=timeout,
            )
        elif kind in ("streaming_dml", "batch_dml", "streaming_ddl"):
            with conn.closing_streaming_cursor() as cur:
                cur.execute(
                    sql,
                    statement_name=statement_name,
                    properties=props,
                    compute_pool_id=pool,
                    timeout=timeout,
                )
                stmt = cur.statement
        else:
            raise StatementManagerError(f"Unsupported SQL kind for {statement_name}: {kind}")
        print(f"Statement {statement_name} results: {stmt}")
        return {
            "name": statement_name,
            "phase": _statement_phase(stmt),
            "detail": _statement_detail(stmt),
            "kind": kind,
        }

    def create_statement(self, statement_name: str, sql: str) -> dict[str, Any]:
        """Create a Flink statement; on 409 conflict delete and retry once."""
        with self.connect() as conn:
            try:
                return self._submit_on_connection(conn, statement_name, sql)
            except OperationalError as exc:
                print(f"OperationalError: {exc}")
                if exc.http_status_code != 409:
                    detail = str(exc)
                    if exc.http_status_code is not None:
                        detail = f"{detail} (HTTP {exc.http_status_code})"
                    raise StatementManagerError(
                        f"Failed to create {statement_name}: {detail}"
                    ) from exc
                try:
                    conn.delete_statement(statement_name)
                except StatementNotFoundError:
                    pass
                deadline = time.monotonic() + self._settings.timeout_seconds
                while time.monotonic() < deadline:
                    try:
                        conn.get_statement(statement_name)
                        time.sleep(self._settings.poll_seconds)
                    except StatementNotFoundError:
                        break
                else:
                    raise StatementManagerError(
                        f"Statement {statement_name} still exists after delete before retry"
                    )
                return self._submit_on_connection(conn, statement_name, sql)

    def wait_for_phase(
        self,
        statement_name: str,
        accepted_phases: set[str] | frozenset[str] | None = None,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Poll until statement reaches an accepted or terminal phase."""
        accepted = accepted_phases or SUCCESS_PHASES
        deadline = time.monotonic() + (timeout or self._settings.timeout_seconds)
        poll = self._settings.poll_seconds
        last: dict[str, Any] = {}

        while time.monotonic() < deadline:
            last = self.get_statement(statement_name)
            phase = last.get("phase", "UNKNOWN")
            if phase in accepted or phase in FAILURE_PHASES or phase == "NOT_FOUND":
                return last
            time.sleep(poll)

        raise StatementManagerError(
            f"Timeout waiting for {statement_name}; last status: {json.dumps(last)}"
        )

    def get_statement_exceptions(self, statement_name: str) -> dict[str, Any]:
        """Fetch recent exceptions for a statement via Flink REST."""
        with self.connect() as conn:
            resp = conn._request(  # noqa: SLF001
                f"/statements/{statement_name}/exceptions",
                method="GET",
                raise_for_status=False,
            )
            status = getattr(resp, "status_code", 200)
            if status == 404:
                return {"name": statement_name, "exceptions": []}
            if isinstance(status, int) and status >= 400:
                return {
                    "name": statement_name,
                    "error": f"HTTP {status}",
                    "body": getattr(resp, "text", str(resp)),
                }
            try:
                return resp.json()
            except Exception:
                return {"name": statement_name, "raw": str(resp)}

    def check_statement_health(self, statement_name: str) -> dict[str, Any]:
        """Simple health summary from statement phase."""
        status = self.get_statement(statement_name)
        phase = status.get("phase", "UNKNOWN")
        healthy = phase in SUCCESS_PHASES
        return {
            "statement_name": statement_name,
            "phase": phase,
            "healthy": healthy,
            "detail": status.get("detail", ""),
        }

    def _wait_for_deploy_phase(self, statement_name: str) -> str:
        try:
            result = self.wait_for_phase(statement_name, SUCCESS_PHASES | FAILURE_PHASES)
        except StatementManagerError as exc:
            raise DeployError(str(exc)) from exc
        return str(result.get("phase", "UNKNOWN"))

    def _deploy_source_ddls(
        self,
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
                self.create_statement(source_name, source_sql)
            except StatementManagerError as exc:
                raise DeployError(
                    f"create-flink-statement failed for {source_name}: {exc}"
                ) from exc

            messages.append(f"Created source DDL statement {source_name}")

            phase = self._wait_for_deploy_phase(source_name)
            messages.append(f"Source DDL {source_name} phase: {phase}")
            source_statements.append((source_name, phase))

            if phase in FAILURE_PHASES:
                exceptions = self.get_statement_exceptions(source_name)
                raise DeployError(
                    f"Source DDL {source_name} failed with phase {phase}: {exceptions}"
                )

        return source_statements

    def _validation_issue(
        self,
        sql: str,
        kind: str,
        index: int,
        message: str,
    ) -> Any:
        from flink_skill_common.sql_validate import SqlValidationIssue

        preview = sql.strip().splitlines()[0][:80] if sql.strip() else ""
        return SqlValidationIssue(
            statement_index=index,
            kind=kind,  # type: ignore[arg-type]
            message=f"{message} [{preview}]",
            severity="error",
        )

    def validate_sql(
        self,
        sql: str,
        *,
        kind: str = "ddl",
        index: int = 0,
        statement_name: str | None = None,
    ) -> list[Any]:
        """Submit SQL to CC Flink with a temporary statement name; delete after check."""
        stripped = sql.strip()
        if not stripped:
            return []

        name = statement_name or f"validate-{uuid.uuid4().hex[:12]}"
        try:
            try:
                result = self.create_statement(name, stripped)
            except StatementManagerError as exc:
                return [self._validation_issue(sql, kind, index, str(exc))]

            phase = str(result.get("phase", "UNKNOWN"))
            if phase in FAILURE_PHASES:
                exceptions = self.get_statement_exceptions(name)
                return [
                    self._validation_issue(
                        sql,
                        kind,
                        index,
                        f"Flink rejected statement (phase={phase}): {json.dumps(exceptions)}",
                    )
                ]

            if phase not in SUCCESS_PHASES:
                try:
                    polled = self.wait_for_phase(
                        name,
                        SUCCESS_PHASES | FAILURE_PHASES,
                        timeout=min(30.0, self._settings.timeout_seconds),
                    )
                    phase = str(polled.get("phase", phase))
                except StatementManagerError as exc:
                    return [self._validation_issue(sql, kind, index, str(exc))]

                if phase in FAILURE_PHASES:
                    exceptions = self.get_statement_exceptions(name)
                    return [
                        self._validation_issue(
                            sql,
                            kind,
                            index,
                            f"Flink rejected statement (phase={phase}): {json.dumps(exceptions)}",
                        )
                    ]
        finally:
            try:
                self.delete_statement(name)
            except StatementManagerError:
                logger.warning("Failed to delete validation statement %s", name)

        return []

    def validate_statements(
        self,
        ddls: list[str],
        dmls: list[str],
    ) -> list[Any]:
        """Validate DDL and DML statement lists on CC Flink."""
        issues: list[Any] = []
        for index, sql in enumerate(ddls):
            issues.extend(self.validate_sql(sql, kind="ddl", index=index))
        for index, sql in enumerate(dmls):
            issues.extend(self.validate_sql(sql, kind="dml", index=index))
        return issues

    def deploy_table(
        self,
        table_name: str,
        ddl_path: Path,
        dml_path: Path,
        tests_dir: Path | None = None,
    ) -> DeployResult:
        """Deploy source DDLs (tests/), target DDL, then DML to Confluent Cloud Flink."""
        ddl_sql = ddl_path.read_text().strip()
        dml_sql = dml_path.read_text().strip() if dml_path.is_file() else ""

        ddl_name = ddl_statement_name(table_name)
        dml_name = dml_statement_name(table_name)
        messages: list[str] = []

        source_statements = self._deploy_source_ddls(tests_dir, messages)

        if not ddl_sql:
            raise DeployError(f"DDL file is empty: {ddl_path}")

        try:
            self.create_statement(ddl_name, ddl_sql)
        except StatementManagerError as exc:
            raise DeployError(f"create-flink-statement failed for {ddl_name}: {exc}") from exc
        messages.append(f"Created DDL statement {ddl_name}")

        ddl_phase = self._wait_for_deploy_phase(ddl_name)
        messages.append(f"DDL {ddl_name} phase: {ddl_phase}")

        if ddl_phase in FAILURE_PHASES:
            exceptions = self.get_statement_exceptions(ddl_name)
            raise DeployError(f"DDL {ddl_name} failed with phase {ddl_phase}: {exceptions}")

        dml_phase = ""
        health = ""
        exceptions = ""

        if dml_sql:
            try:
                self.create_statement(dml_name, dml_sql)
            except StatementManagerError as exc:
                raise DeployError(f"create-flink-statement failed for {dml_name}: {exc}") from exc
            messages.append(f"Created DML statement {dml_name}")

            dml_phase = self._wait_for_deploy_phase(dml_name)
            messages.append(f"DML {dml_name} phase: {dml_phase}")

            if dml_phase in FAILURE_PHASES:
                exceptions = json.dumps(self.get_statement_exceptions(dml_name))
                raise DeployError(f"DML {dml_name} failed with phase {dml_phase}: {exceptions}")

            health_result = self.check_statement_health(dml_name)
            health = json.dumps(health_result)
            messages.append(f"Health: {health[:200]}")

        success = (ddl_phase in SUCCESS_PHASES or ddl_phase == "NOT_FOUND") and (
            not dml_sql or dml_phase in SUCCESS_PHASES or dml_phase == "NOT_FOUND"
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
