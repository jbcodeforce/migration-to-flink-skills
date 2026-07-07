"""
Integration tests for converge_flink_sql workflow branches.
"""

from pathlib import Path

import pytest
import json
from flink_ref_fixtures import (
    load_flink_pair
)
from flink_skill_common.config import configure, HarnessContext
from flink_skill_common.convergence import ConvergenceContext, converge_flink_sql

pytestmark = pytest.mark.integration

TABLE_NAME = "filtered_publications"
__COMMON_ROOT = Path(__file__).resolve().parents[3]
__PROJECT_ROOT = __COMMON_ROOT.parent
configure(HarnessContext(harness_root=__COMMON_ROOT, project_root=__PROJECT_ROOT))
_REFERENCES_ROOT = __PROJECT_ROOT / "references"

def test_load_pair(tmp_path: Path, require_deploy):
    filtering_dir = _REFERENCES_ROOT / "flink" / "valid" / "routing" / "filtering"
    ddls, dmls, src_dir = load_flink_pair(filtering_dir)
    assert len(ddls) == 2
    assert len(dmls) == 1
    assert src_dir.is_dir()


def test_converge_valid_deploy(tmp_path: Path, require_deploy):
    filtering_dir = _REFERENCES_ROOT / "flink" / "valid" / "routing" / "filtering"
    ddls, dmls, src_dir = load_flink_pair(filtering_dir)

    ctx = ConvergenceContext(
        table_name=TABLE_NAME,
        source_sql=dmls[0],
        source_label="fixture",
        out_dir=tmp_path,
        tests_dir=src_dir / "tests",
    )
    result = converge_flink_sql(
        ddls,
        dmls,
        ctx,
        skip_deploy=False,
        agent_on_failure=True,
    )
    print(json.dumps(result.model_dump() if hasattr(result, "model_dump") else result.__dict__, indent=2, default=str))
    assert result.success is True
    assert any("Deploy OK" in msg for msg in result.messages)
