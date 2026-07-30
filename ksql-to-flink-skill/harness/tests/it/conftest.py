"""Integration test harness context (loads repo-root .env)."""

import os
from pathlib import Path

import pytest

from flink_skill_common.config import (
    FlinkDeployNotReadyError,
    HarnessContext,
    cli_log_file,
    configure,
    dotenv_path,
    find_repo_root,
    flink_deploy_settings,
    get_context,
    llm_api_key,
    llm_base_url,
    llm_model,
    llm_reachable,
    load_env,
)

_HARNESS_DIR = Path(__file__).resolve().parents[2]
_SKILL_PACKAGE_ROOT = _HARNESS_DIR.parent
_PROJECT_ROOT = find_repo_root(_HARNESS_DIR)

configure(HarnessContext(harness_root=_SKILL_PACKAGE_ROOT, project_root=_PROJECT_ROOT))



def _mask(value: str | None) -> str:
    if not value:
        return "(empty)"
    return f"{'*' * max(len(value) - 4, 0)}{value[-4:]}"


def _trace_env_used() -> None:
    """Print which dotenv file and env vars the IT harness resolves."""
    load_env()
    ctx = get_context()
    path = dotenv_path()
    print(f"project_root={ctx.project_root}")
    print(f"harness_root={ctx.harness_root}")
    print(f"DOTENV_FILE={os.getenv('DOTENV_FILE') or '(unset)'}")
    print(f"dotenv_path={path if path else '(none)'}")
    print(f"SL_LLM_BASE_URL={llm_base_url()}")
    print(f"SL_LLM_MODEL={llm_model()}")
    print(f"SL_LLM_API_KEY={_mask(llm_api_key())}")
    try:
        settings = flink_deploy_settings()
    except FlinkDeployNotReadyError as exc:
        print(f"flink_deploy_settings=(not ready: {exc})")
        return
    print(f"FLINK_ORG_ID={settings.organization_id}")
    print(f"FLINK_ENV_ID={settings.environment_id}")
    print(f"FLINK_COMPUTE_POOL_ID={settings.compute_pool_id}")
    print(f"FLINK_DATABASE_NAME={settings.database_name}")
    print(f"FLINK_REST_ENDPOINT={settings.endpoint or '(default)'}")
    print(f"CLOUD_PROVIDER={settings.cloud_provider}")
    print(f"CLOUD_REGION={settings.cloud_region}")
    print(f"FLINK_API_KEY={_mask(settings.flink_api_key)}")
    print(f"FLINK_API_SECRET={_mask(settings.flink_api_secret)}")


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
    load_env()
    _trace_env_used()
    base_url = llm_base_url()
    if not llm_reachable():
        pytest.skip(f"LLM not reachable at {base_url}/models")
