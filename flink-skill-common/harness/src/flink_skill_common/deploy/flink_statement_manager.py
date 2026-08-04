"""
Copyright 2024-2026 Confluent, Inc.
KSQL to Flink SQL Translation Agent

Migration-oriented Flink deploy adapter over cc_deploy statement lifecycle.
"""

from __future__ import annotations

import json
import re
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Literal

from cc_deploy.flink_deploy import flink_connection
from cc_deploy.statement_lifecycle import (
    FAILURE_PHASES,
    SUCCESS_PHASES,
    StatementLifecycleError,
    check_statement_health as lifecycle_check_health,
    classify_sql as lifecycle_classify_sql,
    create_statement as lifecycle_create_statement,
    delete_statement as lifecycle_delete_statement,
    drop_table as lifecycle_drop_table,
    get_statement_exceptions as lifecycle_get_exceptions,
    list_statements as lifecycle_list_statements,
    statement_status,
    wait_for_phase as lifecycle_wait_for_phase,
)

from flink_skill_common.cli_interrupt import interruptible_sleep
from flink_skill_common.config import FlinkDeploySettings, flink_deploy_settings, get_logger
from flink_skill_common.sql_parse import extract_statement_table_name

# Re-export phase sets for callers / tests
__all__ = [
    "SUCCESS_PHASES",
    "FAILURE_PHASES",
    "StatementManagerError",
    "DeployError",
    "DeployResult",
    "FlinkStatementManager",
    "classify_sql",
    "normalize_statement_prefix",
    "ddl_statement_name",
    "dml_statement_name",
    "discover_source_ddl_files",
    "settings_to_cc_config",
]


def _logger():
    try:
        return get_logger()
    except RuntimeError:
        import logging

        return logging.getLogger("flink_migration_skill.deploy")


SqlKind = Literal["snapshot_ddl", "streaming_dml", "batch_dml", "streaming_ddl"]

STATEMENT_NAME_RE = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")


def settings_to_cc_config(settings: FlinkDeploySettings) -> dict[str, str]:
    """Map FlinkDeploySettings to cc_deploy connection config dict."""
    cfg: dict[str, str] = {
        "FLINK_API_KEY": settings.flink_api_key,
        "FLINK_API_SECRET": settings.flink_api_secret,
        "ORGANIZATION_ID": settings.organization_id,
        "ENVIRONMENT_ID": settings.environment_id,
        "FLINK_COMPUTE_POOL_ID": settings.compute_pool_id,
        "FLINK_DATABASE_NAME": settings.database_name,
        "CLOUD_PROVIDER": settings.cloud_provider,
        "CLOUD_REGION": settings.cloud_region,
    }
    if settings.endpoint:
        cfg["FLINK_REST_ENDPOINT"] = settings.endpoint.rstrip("/")
    return cfg


def normalize_statement_prefix(table_name: str) -> str:
    """Normalize table name for Flink statement names (hyphens, lowercase)."""
    normalized = table_name.lower().replace("_", "-")
    if not STATEMENT_NAME_RE.match(normalized):
        raise ValueError(
            f"Table name {table_name!r} cannot be normalized to a valid statement name prefix"
        )
    return normalized


def ddl_statement_name(table_name: str) -> str:
    return f"{normalize_statement_prefix(table_name)}-ddl"


def dml_statement_name(table_name: str) -> str:
    return f"{normalize_statement_prefix(table_name)}-dml"


def discover_source_ddl_files(tests_dir: Path) -> list[tuple[str, Path]]:
    """Return (table_name, path) for each tests/*.sql source stub with a table name."""
    if not tests_dir.is_dir():
        return []
    results: list[tuple[str, Path]] = []
    for path in sorted(tests_dir.glob("*.sql")):
        table_name = extract_statement_table_name(path.read_text())
        if table_name:
            results.append((table_name, path))
    return results


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
    return lifecycle_classify_sql(sql)  # type: ignore[return-value]


class FlinkStatementManager:
    """Thin adapter: migration deploy orchestration over cc_deploy lifecycle."""

    def __init__(self, settings: FlinkDeploySettings | None = None) -> None:
        self._settings = settings or flink_deploy_settings()
        self._config = settings_to_cc_config(self._settings)

    @property
    def settings(self) -> FlinkDeploySettings:
        return self._settings

    @property
    def config(self) -> dict[str, str]:
        return self._config

    @contextmanager
    def connect(self) -> Iterator[Any]:
        """Open a confluent-sql connection via cc_deploy."""
        with flink_connection(
            self._config, user_agent=self._settings.http_user_agent
        ) as conn:
            yield conn

    def _wrap(self, exc: StatementLifecycleError) -> StatementManagerError:
        return StatementManagerError(str(exc))

    def get_statement(self, statement_name: str) -> dict[str, Any]:
        """Return normalized statement status."""
        with self.connect() as conn:
            return statement_status(conn, statement_name)

    def list_statements(self, page_size: int = 50) -> dict[str, Any]:
        """List Flink statements (first REST page)."""
        with self.connect() as conn:
            return lifecycle_list_statements(conn, page_size=page_size)

    def _delete_statement_safe(self, statement_name: str) -> None:
        """Delete a statement; log and continue on failure."""
        try:
            self.delete_statement(statement_name)
        except StatementManagerError:
            _logger().warning("Failed to delete statement %s", statement_name)

    def cleanup_deployed_table(
        self,
        table_name: str,
        tests_dir: Path | None = None,
    ) -> None:
        """Delete DML statement and drop target plus source stub tables."""
        self._delete_statement_safe(dml_statement_name(table_name))
        try:
            self.drop_table(table_name)
        except StatementManagerError:
            _logger().warning("Failed to drop table %s", table_name)
        if tests_dir is not None:
            for source_table, _ in discover_source_ddl_files(tests_dir):
                try:
                    self.drop_table(source_table)
                except StatementManagerError:
                    _logger().warning("Failed to drop source table %s", source_table)

    def drop_table(self, table_name: str) -> None:
        """Drop a table via ephemeral statement."""
        tname = table_name.lower().replace("_", "-")
        statement_name = "drop-" + tname
        with self.connect() as conn:
            try:
                lifecycle_drop_table(
                    conn,
                    self._config,
                    table_name,
                    statement_name,
                    timeout=self._settings.timeout_seconds,
                    poll=self._settings.poll_seconds,
                    sleep=interruptible_sleep,
                )
            except StatementLifecycleError as exc:
                raise self._wrap(exc) from exc

    def delete_statement(self, statement_name: str) -> dict[str, Any]:
        """Delete a statement and wait until it is gone."""
        with self.connect() as conn:
            try:
                return lifecycle_delete_statement(
                    conn,
                    statement_name,
                    timeout=self._settings.timeout_seconds,
                    poll=self._settings.poll_seconds,
                    sleep=interruptible_sleep,
                )
            except StatementLifecycleError as exc:
                raise self._wrap(exc) from exc

    def create_statement(
        self,
        statement_name: str,
        sql: str,
        *,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Create a Flink statement; on 409 conflict delete and retry once."""
        with self.connect() as conn:
            try:
                result = lifecycle_create_statement(
                    conn,
                    self._config,
                    statement_name,
                    sql,
                    dry_run=dry_run,
                    timeout=self._settings.timeout_seconds,
                    poll=self._settings.poll_seconds,
                    sleep=interruptible_sleep,
                )
            except StatementLifecycleError as exc:
                _logger().warning("Error creating %s: %s", statement_name, exc)
                raise self._wrap(exc) from exc
            _logger().info(
                "Statement %s submitted (kind=%s, phase=%s)",
                statement_name,
                result.get("kind"),
                result.get("phase"),
            )
            return result

    def wait_for_phase(
        self,
        statement_name: str,
        accepted_phases: set[str] | frozenset[str] | None = None,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Poll until statement reaches an accepted or terminal phase."""
        accepted = accepted_phases or SUCCESS_PHASES
        with self.connect() as conn:
            try:
                last = lifecycle_wait_for_phase(
                    conn,
                    statement_name,
                    accepted,
                    timeout=timeout if timeout is not None else self._settings.timeout_seconds,
                    poll=self._settings.poll_seconds,
                    sleep=interruptible_sleep,
                    treat_failure_as_terminal=True,
                )
            except StatementLifecycleError as exc:
                raise self._wrap(exc) from exc
            _logger().info(
                "Statement %s reached phase %s (detail=%s)",
                statement_name,
                last.get("phase"),
                last.get("detail", ""),
            )
            return last

    def get_statement_exceptions(self, statement_name: str) -> dict[str, Any]:
        """Fetch recent exceptions for a statement via Flink REST."""
        with self.connect() as conn:
            return lifecycle_get_exceptions(conn, statement_name)

    def check_statement_health(self, statement_name: str) -> dict[str, Any]:
        """Simple health summary from statement phase."""
        with self.connect() as conn:
            return lifecycle_check_health(
                conn, statement_name, success_phases=SUCCESS_PHASES
            )

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
        """Deploy source stub DDLs from tests/*.sql before target statements."""
        source_statements: list[tuple[str, str]] = []
        if tests_dir is None:
            return source_statements

        for source_table, source_path in discover_source_ddl_files(tests_dir):
            source_sql = source_path.read_text().strip()
            if not source_sql:
                continue
            source_name = ddl_statement_name(source_table)
            _logger().info("Deploying source DDL %s from %s", source_name, source_path)
            try:
                self.create_statement(source_name, source_sql, dry_run=False)
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

            self._delete_statement_safe(source_name)

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
        """
        Submit SQL to CC Flink with dry-run and a temporary statement name;
        delete after check.
        """
        stripped = sql.strip()
        if not stripped:
            return []

        name = statement_name or f"validate-{uuid.uuid4().hex[:12]}"
        try:
            try:
                result = self.create_statement(name, stripped, dry_run=True)
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
                _logger().warning("Failed to delete validation statement %s", name)

        return []

    def validate_statements(
        self,
        ddls: list[str],
        dmls: list[str],
    ) -> list[Any]:
        """Validate DDL and DML statement lists on CC Flink (dry-run)."""
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
        """
        Deploy source DDLs (tests/), target DDL,
        then DML to Confluent Cloud Flink (real deploy, no dry-run).
        """
        ddl_sql = ddl_path.read_text().strip()
        dml_sql = dml_path.read_text().strip() if dml_path.is_file() else ""

        ddl_name = ddl_statement_name(table_name)
        dml_name = dml_statement_name(table_name)
        messages: list[str] = []

        source_statements = self._deploy_source_ddls(tests_dir, messages)

        if not ddl_sql:
            raise DeployError(f"DDL file is empty: {ddl_path}")

        _logger().info("Deploying target DDL %s from %s", ddl_name, ddl_path)
        try:
            self.create_statement(ddl_name, ddl_sql, dry_run=False)
        except StatementManagerError as exc:
            raise DeployError(f"create-flink-statement failed for {ddl_name}: {exc}") from exc
        messages.append(f"Created DDL statement {ddl_name}")

        ddl_phase = self._wait_for_deploy_phase(ddl_name)
        messages.append(f"DDL {ddl_name} phase: {ddl_phase}")

        if ddl_phase in FAILURE_PHASES:
            exceptions = self.get_statement_exceptions(ddl_name)
            raise DeployError(f"DDL {ddl_name} failed with phase {ddl_phase}: {exceptions}")

        self._delete_statement_safe(ddl_name)

        dml_phase = ""
        health = ""
        exceptions = ""

        if dml_sql:
            _logger().info("Deploying target DML %s from %s", dml_name, dml_path)
            try:
                self.create_statement(dml_name, dml_sql, dry_run=False)
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
