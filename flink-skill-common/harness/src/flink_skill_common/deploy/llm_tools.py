"""
Copyright 2024-2026 Confluent, Inc.
KSQL to Flink SQL Translation Agent

Agno tool wrappers for Flink statement management.
"""

from __future__ import annotations

import json
from typing import Any, Callable

from flink_skill_common.deploy.flink_statement_manager import (
    FAILURE_PHASES,
    SUCCESS_PHASES,
    FlinkStatementManager,
)


class FlinkStatementLLMTools:
    """Expose FlinkStatementManager operations as Agno agent tools."""

    def __init__(self, manager: FlinkStatementManager | None = None) -> None:
        self._manager = manager or FlinkStatementManager()

    @property
    def manager(self) -> FlinkStatementManager:
        return self._manager

    def create_flink_statement(self, statement_name: str, sql: str) -> str:
        """Create a Flink SQL statement on Confluent Cloud (DDL or DML)."""
        result = self._manager.create_statement(statement_name, sql)
        return json.dumps(result, indent=2)

    def get_flink_statement(self, statement_name: str) -> str:
        """Get phase and detail for a Flink statement by name."""
        return json.dumps(self._manager.get_statement(statement_name), indent=2)

    def list_flink_statements(self, page_size: int = 50) -> str:
        """List Flink statements in the environment."""
        return json.dumps(self._manager.list_statements(page_size=page_size), indent=2)

    def delete_flink_statement(self, statement_name: str) -> str:
        """Delete a Flink statement by name."""
        return json.dumps(self._manager.delete_statement(statement_name), indent=2)

    def get_flink_statement_exceptions(self, statement_name: str) -> str:
        """Get recent exceptions for a failed Flink statement."""
        return json.dumps(self._manager.get_statement_exceptions(statement_name), indent=2)

    def wait_flink_statement_phase(
        self,
        statement_name: str,
        accepted_phases: str = "RUNNING,COMPLETED,APPLIED,STOPPED",
    ) -> str:
        """Wait until a statement reaches one of the accepted phases (comma-separated)."""
        phases = {p.strip().upper() for p in accepted_phases.split(",") if p.strip()}
        result = self._manager.wait_for_phase(statement_name, phases)
        return json.dumps(result, indent=2)

    def check_flink_statement_health(self, statement_name: str) -> str:
        """Check whether a Flink statement is in a healthy running phase."""
        return json.dumps(self._manager.check_statement_health(statement_name), indent=2)

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
