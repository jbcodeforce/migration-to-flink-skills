"""Shared harness root paths for ksql-to-flink-skill unit tests."""

from __future__ import annotations

from pathlib import Path

from flink_skill_common.config import HarnessContext, configure

_UT_DIR = Path(__file__).resolve().parent
_SKILL_PACKAGE_ROOT = _UT_DIR.parents[2].parent


class HarnessUtBase:
    HARNESS_ROOT = _SKILL_PACKAGE_ROOT
    PROJECT_ROOT = _SKILL_PACKAGE_ROOT.parent

    @classmethod
    def configure_harness(cls) -> None:
        configure(
            HarnessContext(
                harness_root=cls.HARNESS_ROOT,
                project_root=cls.PROJECT_ROOT,
            )
        )


HARNESS_ROOT = HarnessUtBase.HARNESS_ROOT
PROJECT_ROOT = HarnessUtBase.PROJECT_ROOT
