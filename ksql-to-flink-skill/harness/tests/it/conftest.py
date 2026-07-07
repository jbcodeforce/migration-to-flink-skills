"""Integration test harness context (loads repo-root .env)."""

import ksql_to_flink.config  # noqa: F401 — configure shared harness context

import pytest

from flink_skill_common.config import (
    FlinkDeployNotReadyError,
    cli_log_file,
    flink_deploy_settings,
    llm_reachable,
)


@pytest.fixture(autouse=True)
def _clear_logs_file():
    log_file = cli_log_file()
    try:
        if log_file.exists():
            log_file.unlink()
    except Exception:
        pass


@pytest.fixture
def require_deploy():
    try:
        flink_deploy_settings()
    except FlinkDeployNotReadyError as exc:
        pytest.skip(f"Flink deploy not configured: {exc}")


@pytest.fixture
def require_llm():
    if not llm_reachable():
        pytest.skip("LLM not reachable")
