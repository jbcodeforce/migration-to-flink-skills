"""Environment and fixture path configuration."""

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


def c360_spark_root() -> Path:
    load_env()
    raw = os.getenv(
        "C360_SPARK_ROOT",
        str(get_context().code_root / "flink_project_demos/customer_360/c360_spark_processing"),
    )
    return Path(raw).resolve()


def c360_flink_root() -> Path:
    load_env()
    raw = os.getenv(
        "C360_FLINK_ROOT",
        str(get_context().code_root / "flink_project_demos/customer_360/c360_flink_processing"),
    )
    return Path(raw).resolve()


def spark_project_root() -> Path:
    load_env()
    raw = os.getenv(
        "SPARK_PROJECT_ROOT",
        str(get_context().code_root / "flink_project_demos/spark-project"),
    )
    return Path(raw).resolve()


def skill_dir() -> Path:
    return get_context().skill_dir


def skill_md_path() -> Path:
    return get_context().skill_md_path
