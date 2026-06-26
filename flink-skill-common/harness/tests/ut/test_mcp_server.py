"""Unit tests for flink-skill MCP server tools."""

from __future__ import annotations

import json

from pathlib import Path

import pytest

from flink_skill_common.config import HarnessContext, configure
from flink_skill_common.mcp import server as mcp_server


@pytest.fixture(autouse=True)
def _configure_harness():
    harness_root = Path(__file__).resolve().parents[2]
    project_root = harness_root.parent
    configure(HarnessContext(harness_root=harness_root, project_root=project_root))
    mcp_server._deploy_tools = None
    yield
    mcp_server._deploy_tools = None


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
