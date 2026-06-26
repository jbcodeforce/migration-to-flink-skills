"""Cursor MCP server for Flink SQL validation and Confluent Cloud deploy."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

from flink_skill_common.config import (
    FlinkDeployNotReadyError,
    HarnessContext,
    configure,
    load_env,
)
from flink_skill_common.deploy.llm_tools import FlinkStatementLLMTools
from flink_skill_common.sql_validate import (
    SqlValidationIssue,
    validate_statements_remote,
    validate_syntax_for_statements,
)

mcp = FastMCP("flink-skill-common")

_deploy_tools: FlinkStatementLLMTools | None = None


def _bootstrap_env() -> None:
    harness_root = Path(__file__).resolve().parents[3]
    project_root = harness_root.parent
    configure(HarnessContext(harness_root=harness_root, project_root=project_root))
    load_env()


def _get_deploy_tools() -> FlinkStatementLLMTools:
    global _deploy_tools
    if _deploy_tools is None:
        _bootstrap_env()
        _deploy_tools = FlinkStatementLLMTools()
    return _deploy_tools


def _issue_dict(issue: SqlValidationIssue) -> dict[str, Any]:
    return {
        "statement_index": issue.statement_index,
        "kind": issue.kind,
        "message": issue.message,
        "line": issue.line,
        "severity": issue.severity,
    }


def _validation_result(issues: list[SqlValidationIssue]) -> str:
    errors = [issue for issue in issues if issue.severity == "error"]
    payload = {
        "ok": not errors,
        "issues": [_issue_dict(issue) for issue in issues],
        "error_count": len(errors),
    }
    return json.dumps(payload, indent=2)


def _error_payload(message: str) -> str:
    return json.dumps({"ok": False, "error": message}, indent=2)


@mcp.tool()
def validate_flink_sql_offline(ddls: list[str], dmls: list[str] | None = None) -> str:
    """Validate Flink DDL/DML offline using sqlglot (Flink dialect)."""
    _bootstrap_env()
    issues = validate_syntax_for_statements(ddls, dmls or [])
    return _validation_result(issues)


@mcp.tool()
def validate_flink_sql_remote(ddls: list[str], dmls: list[str] | None = None) -> str:
    """Validate Flink DDL/DML using the Confluent Cloud Flink parser."""
    _bootstrap_env()
    try:
        issues = validate_statements_remote(ddls, dmls or [])
    except FlinkDeployNotReadyError as exc:
        return _error_payload(str(exc))
    return _validation_result(issues)


@mcp.tool()
def create_flink_statement(statement_name: str, sql: str) -> str:
    """Create a Flink SQL statement on Confluent Cloud (DDL or DML)."""
    return _get_deploy_tools().create_flink_statement(statement_name, sql)


@mcp.tool()
def wait_flink_statement_phase(
    statement_name: str,
    accepted_phases: str = "RUNNING,COMPLETED,APPLIED,STOPPED",
) -> str:
    """Wait until a Flink statement reaches one of the accepted phases (comma-separated)."""
    return _get_deploy_tools().wait_flink_statement_phase(statement_name, accepted_phases)


@mcp.tool()
def get_flink_statement_exceptions(statement_name: str) -> str:
    """Get recent exceptions for a failed Flink statement."""
    return _get_deploy_tools().get_flink_statement_exceptions(statement_name)


@mcp.tool()
def check_flink_statement_health(statement_name: str) -> str:
    """Check whether a Flink statement is in a healthy running phase."""
    return _get_deploy_tools().check_flink_statement_health(statement_name)


def main() -> None:
    _bootstrap_env()
    mcp.run()


if __name__ == "__main__":
    main()
