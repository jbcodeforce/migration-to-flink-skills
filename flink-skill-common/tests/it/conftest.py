"""Integration test harness context (loads flink-skill-common/.env)."""

from pathlib import Path

import pytest

from flink_skill_common.config import HarnessContext, configure

HARNESS_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def _configure_harness_context():
    configure(HarnessContext(harness_root=HARNESS_ROOT, project_root=HARNESS_ROOT))
