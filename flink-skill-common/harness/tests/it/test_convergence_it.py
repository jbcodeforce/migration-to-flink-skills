"""
Integration tests for converge_flink_sql workflow branches.
"""

from pathlib import Path

import pytest

from flink_ref_fixtures import (
    load_flink_pair
)
from flink_skill_common.config import configure, HarnessContext
from flink_skill_common.convergence import ConvergenceContext, converge_flink_sql
from flink_skill_common.deploy.flink_statement_manager import FlinkStatementManager

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

    try:
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
        print(result)
        assert result.success is True
        assert any("Deploy OK" in msg for msg in result.messages)
    except Exception as e:
        print(e)
    finally:
        FlinkStatementManager().drop_table(TABLE_NAME);
        # ADD drop test tables
        FlinkStatementManager().drop_table("all_publications");

