from __future__ import annotations

import os
from pathlib import Path

from flink_skill_common.config import (
    HarnessContext,
    configure,
    get_context,
    llm_api_key,
    llm_base_url,
    llm_model,
    load_env,
)

_HARNESS_ROOT = Path(__file__).resolve().parents[2]
_PROJECT_ROOT = _HARNESS_ROOT.parent

configure(HarnessContext(harness_root=_HARNESS_ROOT, project_root=_PROJECT_ROOT))


def ksql_tutorial_root() -> Path:
    load_env()
    raw = os.getenv(
        "KSQL_TUTORIAL_ROOT",
        str(get_context().code_root / "flink_project_demos/ksql_tutorial"),
    )
    return Path(raw).resolve()


def ksql_sources_root() -> Path:
    return ksql_tutorial_root() / "sources"


def flink_ref_root() -> Path:
    return ksql_tutorial_root() / "flink_ref"


def skill_dir() -> Path:
    return get_context().skill_dir


def skill_md_path() -> Path:
    return get_context().skill_md_path


def agent_deploy_on_failure() -> bool:
    load_env()
    return os.getenv("KSQL_FLINK_AGENT_DEPLOY", "").lower() in ("1", "true", "yes")


def agent_deploy_max_retries() -> int:
    load_env()
    return int(os.getenv("KSQL_FLINK_AGENT_DEPLOY_MAX_RETRIES", "2"))
