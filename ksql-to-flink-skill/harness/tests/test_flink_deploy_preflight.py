"""Offline tests for Flink deploy preflight checks."""

import pytest

from ksql_flink_skill.deploy import FlinkDeployNotReadyError, require_flink_deploy_ready


def test_require_flink_deploy_ready_missing_credentials(monkeypatch):
    # Empty strings prevent load_dotenv from repopulating credentials from harness/.env
    for name in (
        "FLINK_API_KEY",
        "FLINK_API_SECRET",
        "CONFLUENT_CLOUD_API_KEY",
        "CONFLUENT_CLOUD_API_SECRET",
    ):
        monkeypatch.setenv(name, "")
    with pytest.raises(FlinkDeployNotReadyError, match="FLINK_API_KEY"):
        require_flink_deploy_ready()


def test_require_flink_deploy_ready_with_env(monkeypatch):
    monkeypatch.setenv("FLINK_API_KEY", "key")
    monkeypatch.setenv("FLINK_API_SECRET", "secret")
    monkeypatch.setenv("FLINK_ORG_ID", "org")
    monkeypatch.setenv("FLINK_ENV_ID", "env")
    monkeypatch.setenv("FLINK_COMPUTE_POOL_ID", "pool")
    monkeypatch.setenv("FLINK_DATABASE_NAME", "db")
    settings = require_flink_deploy_ready()
    assert settings.flink_api_key == "key"
