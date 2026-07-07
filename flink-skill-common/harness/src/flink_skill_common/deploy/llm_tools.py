"""
Copyright 2024-2026 Confluent, Inc.
KSQL to Flink SQL Translation Agent

Agno tool wrappers for Flink statement management.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from flink_skill_common.config import get_logger
from flink_skill_common.deploy.flink_statement_manager import (
    FAILURE_PHASES,
    SUCCESS_PHASES,
    DeployError,
    FlinkStatementManager,
    StatementManagerError,
)
from flink_skill_common.user_errors import format_user_error


def _tool_error(tool_name: str, exc: BaseException, **context: Any) -> str:
    get_logger().exception("%s failed", tool_name, exc_info=exc)
    payload = {"error": format_user_error(exc), "tool": tool_name, **context}
    return json.dumps(payload, indent=2)


class FlinkStatementLLMTools:
    """Expose FlinkStatementManager operations as Agno agent tools."""

    def __init__(self, manager: FlinkStatementManager | None = None) -> None:
        self._manager = manager or FlinkStatementManager()

    @property
    def manager(self) -> FlinkStatementManager:
        return self._manager

    def _run_tool(self, tool_name: str, fn: Callable[[], Any], **context: Any) -> str:
        try:
            return json.dumps(fn(), indent=2)
        except (StatementManagerError, DeployError) as exc:
            return _tool_error(tool_name, exc, **context)

    def create_flink_statement(self, statement_name: str, sql: str) -> str:
        """Create a Flink SQL statement on Confluent Cloud (DDL or DML)."""
        return self._run_tool(
            "create_flink_statement",
            lambda: self._manager.create_statement(statement_name, sql),
            statement_name=statement_name,
        )

    def get_flink_statement(self, statement_name: str) -> str:
        """Get phase and detail for a Flink statement by name."""
        return self._run_tool(
            "get_flink_statement",
            lambda: self._manager.get_statement(statement_name),
            statement_name=statement_name,
        )

    def list_flink_statements(self, page_size: int = 50) -> str:
        """List Flink statements in the environment."""
        return self._run_tool(
            "list_flink_statements",
            lambda: self._manager.list_statements(page_size=page_size),
        )

    def delete_flink_statement(self, statement_name: str) -> str:
        """Delete a Flink statement by name."""
        return self._run_tool(
            "delete_flink_statement",
            lambda: self._manager.delete_statement(statement_name),
            statement_name=statement_name,
        )

    def get_flink_statement_exceptions(self, statement_name: str) -> str:
        """Get recent exceptions for a failed Flink statement."""
        return self._run_tool(
            "get_flink_statement_exceptions",
            lambda: self._manager.get_statement_exceptions(statement_name),
            statement_name=statement_name,
        )

    def wait_flink_statement_phase(
        self,
        statement_name: str,
        accepted_phases: str = "RUNNING,COMPLETED,APPLIED,STOPPED",
    ) -> str:
        """Wait until a statement reaches one of the accepted phases (comma-separated)."""
        phases = {p.strip().upper() for p in accepted_phases.split(",") if p.strip()}

        def _wait() -> dict[str, Any]:
            return self._manager.wait_for_phase(statement_name, phases)

        return self._run_tool(
            "wait_flink_statement_phase",
            _wait,
            statement_name=statement_name,
        )

    def check_flink_statement_health(self, statement_name: str) -> str:
        """Check whether a Flink statement is in a healthy running phase."""
        return self._run_tool(
            "check_flink_statement_health",
            lambda: self._manager.check_statement_health(statement_name),
            statement_name=statement_name,
        )

    def as_tools(self) -> list[Callable[..., str]]:
        """Return callables suitable for Agent(tools=...)."""
        return [
            self.create_flink_statement,
            self.get_flink_statement,
            self.list_flink_statements,
            self.delete_flink_statement,
            self.get_flink_statement_exceptions,
            self.wait_flink_statement_phase,
            self.check_flink_statement_health,
        ]

    @staticmethod
    def default_success_phases() -> frozenset[str]:
        return SUCCESS_PHASES

    @staticmethod
    def default_failure_phases() -> frozenset[str]:
        return FAILURE_PHASES
