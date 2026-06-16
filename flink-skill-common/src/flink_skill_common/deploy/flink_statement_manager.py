"""Flink statement lifecycle via confluent-sql REST driver."""

from __future__ import annotations

import json
import time
from contextlib import contextmanager
from typing import Any, Iterator, Literal

import confluent_sql
from confluent_sql.exceptions import OperationalError, StatementNotFoundError

from flink_skill_common.config import FlinkDeploySettings, flink_deploy_settings

SqlKind = Literal["snapshot_ddl", "streaming_dml", "batch_dml", "streaming_ddl"]

SUCCESS_PHASES = frozenset({"RUNNING", "COMPLETED", "APPLIED", "STOPPED"})
FAILURE_PHASES = frozenset({"FAILED", "FAILING", "DELETING"})


class StatementManagerError(RuntimeError):
    """Flink statement operation failed."""


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
    phase = getattr(stmt, "phase", None)
    if phase is not None and hasattr(phase, "name"):
        return str(phase.name)
    if phase is not None and hasattr(phase, "value"):
        return str(phase.value)
    status = getattr(stmt, "status", None)
    if isinstance(status, dict) and status.get("phase"):
        return str(status["phase"])
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
