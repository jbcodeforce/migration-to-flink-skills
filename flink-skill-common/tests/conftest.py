from pathlib import Path

import pytest

from flink_skill_common.config import HarnessContext, configure


@pytest.fixture(autouse=True)
def _configure_harness_context(tmp_path: Path):
    configure(HarnessContext(harness_root=tmp_path, project_root=tmp_path))
