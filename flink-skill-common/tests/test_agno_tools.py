"""Tests for FlinkStatementAgnoTools."""

import json
from unittest.mock import MagicMock

from flink_skill_common.deploy.agno_tools import FlinkStatementAgnoTools


def test_agno_tools_delegate_to_manager():
    manager = MagicMock()
    manager.create_statement.return_value = {"name": "t-ddl", "phase": "RUNNING"}
    tools = FlinkStatementAgnoTools(manager)

    raw = tools.create_flink_statement("t-ddl", "CREATE TABLE t (id STRING);")
    parsed = json.loads(raw)

    assert parsed["phase"] == "RUNNING"
    manager.create_statement.assert_called_once_with("t-ddl", "CREATE TABLE t (id STRING);")


def test_as_tools_returns_callables():
    manager = MagicMock()
    tools = FlinkStatementAgnoTools(manager)
    names = {fn.__name__ for fn in tools.as_tools()}
    assert "create_flink_statement" in names
    assert "get_flink_statement_exceptions" in names
