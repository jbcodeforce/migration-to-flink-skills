"""Integration test harness context (loads repo-root .env)."""

from pathlib import Path

import pytest

from flink_skill_common.config import (
    FlinkDeployNotReadyError,
    HarnessContext,
    cli_log_file,
    configure,
    flink_deploy_settings,
    llm_reachable,
)


HARNESS_ROOT = Path(__file__).resolve().parents[2]
REPO_ROOT = Path(__file__).resolve().parents[4]


@pytest.fixture(autouse=True)
def _configure_harness_context():
    configure(HarnessContext(harness_root=HARNESS_ROOT, project_root=REPO_ROOT))


@pytest.fixture(autouse=True)
def _clear_logs_file(_configure_harness_context):
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
