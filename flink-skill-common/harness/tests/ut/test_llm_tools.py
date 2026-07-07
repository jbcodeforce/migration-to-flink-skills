import json
from unittest.mock import MagicMock

import pytest

from flink_skill_common.config import HarnessContext, configure
from flink_skill_common.deploy.flink_statement_manager import (
    DeployError,
    StatementManagerError,
)
from flink_skill_common.deploy.llm_tools import FlinkStatementLLMTools

_COMMON_ROOT = __import__("pathlib").Path(__file__).resolve().parents[2]
_PROJECT_ROOT = _COMMON_ROOT.parent
configure(HarnessContext(harness_root=_COMMON_ROOT, project_root=_PROJECT_ROOT))


@pytest.fixture
def manager():
    return MagicMock()


@pytest.fixture
def tools(manager):
    return FlinkStatementLLMTools(manager=manager)


def test_create_flink_statement_returns_json_error_instead_of_raising(tools, manager):
    manager.create_statement.side_effect = StatementManagerError(
        "Failed to create clicks-ddl: Schema Registry subject 'clicks-key' doesn't match."
    )

    payload = json.loads(tools.create_flink_statement("clicks-ddl", "CREATE TABLE clicks ..."))

    assert payload["error"] == "Schema Registry subject 'clicks-key' doesn't match."
    assert payload["tool"] == "create_flink_statement"
    assert payload["statement_name"] == "clicks-ddl"


def test_create_flink_statement_success(tools, manager):
    manager.create_statement.return_value = {"name": "clicks-ddl", "phase": "COMPLETED"}

    payload = json.loads(tools.create_flink_statement("clicks-ddl", "CREATE TABLE clicks ..."))

    assert payload["phase"] == "COMPLETED"


def test_wait_flink_statement_phase_returns_deploy_error_json(tools, manager):
    manager.wait_for_phase.side_effect = DeployError("Timeout waiting for clicks-ddl")

    payload = json.loads(tools.wait_flink_statement_phase("clicks-ddl"))

    assert payload["error"] == "Timeout waiting for clicks-ddl"
    assert payload["tool"] == "wait_flink_statement_phase"
