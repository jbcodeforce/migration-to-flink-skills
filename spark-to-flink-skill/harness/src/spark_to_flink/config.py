"""Spark harness configuration — delegates to flink_skill_common."""

from __future__ import annotations

from pathlib import Path

from flink_skill_common.config import (
    HarnessContext,
    configure,
    llm_api_key,
    llm_base_url,
    load_env,
    skill_dir,
)

_HARNESS_DIR = Path(__file__).resolve().parents[2]
_SKILL_PACKAGE_ROOT = _HARNESS_DIR.parent
_PROJECT_ROOT = _SKILL_PACKAGE_ROOT.parent
configure(
    HarnessContext(
        harness_root=_SKILL_PACKAGE_ROOT,
        project_root=_PROJECT_ROOT,
    )
)


__all__ = [
    "configure",
    "llm_api_key",
    "llm_base_url",
    "load_env",
    "skill_dir",
]
