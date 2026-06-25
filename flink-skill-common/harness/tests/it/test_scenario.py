"""Smoke integration tests for Flink deploy connectivity."""

import pytest

from flink_skill_common.config import flink_deploy_settings
from flink_skill_common.deploy.flink_statement_manager import FlinkStatementManager

pytestmark = pytest.mark.integration


def test_validate_config(require_deploy):
    """Validate Flink deploy settings are present."""
    config = flink_deploy_settings()
    assert config.flink_api_key is not None
    assert config.flink_api_secret is not None
    assert config.organization_id is not None
    assert config.environment_id is not None
    assert config.compute_pool_id is not None
    assert config.database_name is not None
    assert config.endpoint is not None
    assert config.cloud_provider is not None
    assert config.cloud_region is not None
    assert config.poll_seconds is not None
    assert config.timeout_seconds is not None
    assert config.http_user_agent is not None


def test_list_statements(require_deploy):
    """List statements in the Flink environment."""
    manager = FlinkStatementManager()
    result = manager.list_statements()
    statements = result["statements"]
    count = result["count"]
    assert len(statements) == count
    assert count >= 0
