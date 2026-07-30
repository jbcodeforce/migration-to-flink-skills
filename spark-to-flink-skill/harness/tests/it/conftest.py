"""Integration test harness context (loads repo-root .env)."""

from pathlib import Path

import pytest

from flink_skill_common.config import (
    HarnessContext,
    configure,
    find_repo_root,
    llm_base_url,
    llm_reachable,
    load_env,
)

_HARNESS_DIR = Path(__file__).resolve().parents[2]
_SKILL_PACKAGE_ROOT = _HARNESS_DIR.parent
_PROJECT_ROOT = find_repo_root(_HARNESS_DIR)

configure(HarnessContext(harness_root=_SKILL_PACKAGE_ROOT, project_root=_PROJECT_ROOT))


@pytest.fixture
def require_llm():
    load_env()
    base_url = llm_base_url()
    if not llm_reachable():
        pytest.skip(f"LLM not reachable at {base_url}/models")
