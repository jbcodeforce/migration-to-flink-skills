"""Integration test harness context (loads repo-root .env)."""

from pathlib import Path
import os
import pytest
from flink_skill_common.config import cli_log_file
from flink_skill_common.config import FlinkDeployNotReadyError, HarnessContext, configure, flink_deploy_settings
from flink_skill_common.llm import llm_reachable

HARNESS_ROOT = Path(__file__).resolve().parents[2]
REPO_ROOT = Path(__file__).resolve().parents[4]
FLINK_REF = REPO_ROOT / "references/flink"

@pytest.fixture(autouse=True)
def _configure_harness_context():
    configure(HarnessContext(harness_root=HARNESS_ROOT, project_root=HARNESS_ROOT.parent))

@pytest.fixture(autouse=True)
def _clear_logs_file(_configure_harness_context):
    log_file = cli_log_file()
    try:
        if log_file.exists():
            log_file.unlink()
    except Exception as e:
        # Don't fail the suite if log cleanup fails
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
