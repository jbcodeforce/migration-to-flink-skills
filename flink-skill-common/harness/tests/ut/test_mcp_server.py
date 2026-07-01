"""Unit tests for flink-skill MCP server tools."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from flink_skill_common.config import FlinkDeployNotReadyError, HarnessContext, configure
from flink_skill_common.mcp import server as mcp_server
from flink_skill_common.sql_validate import SqlValidationIssue


@pytest.fixture(autouse=True)
def _configure_harness():
    harness_root = Path(__file__).resolve().parents[2]
    project_root = harness_root.parent
    configure(HarnessContext(harness_root=harness_root, project_root=project_root))
    mcp_server._deploy_tools = None
    yield
    mcp_server._deploy_tools = None


@pytest.fixture
def mock_deploy_tools():
    tools = MagicMock()
    with patch.object(mcp_server, "_get_deploy_tools", return_value=tools):
        yield tools


def test_validate_flink_sql_offline_rejects_bad_dml():
    result = json.loads(
        mcp_server.validate_flink_sql_offline(
            ddls=["CREATE TABLE t (id INT)"],
            dmls=["INSRT INTO t SELECT id FROM s"],
        )
    )
    assert result["ok"] is False
    assert result["error_count"] >= 1
    assert any("INSERT INTO" in issue["message"] for issue in result["issues"])


def test_validate_flink_sql_offline_accepts_minimal_dml():
    result = json.loads(
        mcp_server.validate_flink_sql_offline(
            ddls=[],
            dmls=["INSERT INTO t SELECT id FROM s"],
        )
    )
    assert result["ok"] is True
    assert result["error_count"] == 0


@patch("flink_skill_common.mcp.server.validate_statements_remote")
def test_validate_flink_sql_remote_returns_issues(mock_remote):
    mock_remote.return_value = [
        SqlValidationIssue(0, "ddl", "remote parse error", line=2, severity="error"),
    ]
    result = json.loads(
        mcp_server.validate_flink_sql_remote(
            ddls=["CREATE TABLE t (id INT)"],
            dmls=[],
        )
    )
    assert result["ok"] is False
    assert result["error_count"] == 1
    assert result["issues"][0]["message"] == "remote parse error"
    assert result["issues"][0]["line"] == 2
    mock_remote.assert_called_once_with(["CREATE TABLE t (id INT)"], [])


@patch("flink_skill_common.mcp.server.validate_statements_remote")
def test_validate_flink_sql_remote_deploy_not_ready(mock_remote):
    mock_remote.side_effect = FlinkDeployNotReadyError("missing FLINK_API_KEY")
    result = json.loads(mcp_server.validate_flink_sql_remote(ddls=[], dmls=[]))
    assert result["ok"] is False
    assert result["error"] == "missing FLINK_API_KEY"
    assert "issues" not in result


def test_create_flink_statement(mock_deploy_tools):
    mock_deploy_tools.create_flink_statement.return_value = '{"name": "t-ddl", "phase": "PENDING"}'
    result = json.loads(
        mcp_server.create_flink_statement("t-ddl", "CREATE TABLE t (id INT);")
    )
    assert result["name"] == "t-ddl"
    mock_deploy_tools.create_flink_statement.assert_called_once_with(
        "t-ddl",
        "CREATE TABLE t (id INT);",
    )


def test_wait_flink_statement_phase(mock_deploy_tools):
    mock_deploy_tools.wait_flink_statement_phase.return_value = '{"name": "t-dml", "phase": "RUNNING"}'
    result = json.loads(
        mcp_server.wait_flink_statement_phase("t-dml", "RUNNING,COMPLETED")
    )
    assert result["phase"] == "RUNNING"
    mock_deploy_tools.wait_flink_statement_phase.assert_called_once_with(
        "t-dml",
        "RUNNING,COMPLETED",
    )


def test_get_flink_statement_exceptions(mock_deploy_tools):
    mock_deploy_tools.get_flink_statement_exceptions.return_value = (
        '{"exceptions": [{"message": "boom"}]}'
    )
    result = json.loads(mcp_server.get_flink_statement_exceptions("t-dml"))
    assert result["exceptions"][0]["message"] == "boom"
    mock_deploy_tools.get_flink_statement_exceptions.assert_called_once_with("t-dml")


def test_check_flink_statement_health(mock_deploy_tools):
    mock_deploy_tools.check_flink_statement_health.return_value = (
        '{"healthy": true, "phase": "RUNNING"}'
    )
    result = json.loads(mcp_server.check_flink_statement_health("t-dml"))
    assert result["healthy"] is True
    mock_deploy_tools.check_flink_statement_health.assert_called_once_with("t-dml")


def test_get_deploy_tools_caches_instance():
    tools = MagicMock()
    with patch.object(mcp_server, "FlinkStatementLLMTools", return_value=tools):
        mcp_server._deploy_tools = None
        first = mcp_server._get_deploy_tools()
        second = mcp_server._get_deploy_tools()
    assert first is second
    assert first is tools


def test_mcp_registers_expected_tool_names():
    tool_names = set(mcp_server.mcp._tool_manager._tools.keys())
    expected = {
        "validate_flink_sql_offline",
        "validate_flink_sql_remote",
        "create_flink_statement",
        "wait_flink_statement_phase",
        "get_flink_statement_exceptions",
        "check_flink_statement_health",
    }
    assert expected.issubset(tool_names)


@patch.object(mcp_server.mcp, "run")
def test_main_runs_mcp_server(mock_run):
    mcp_server.main()
    mock_run.assert_called_once()
