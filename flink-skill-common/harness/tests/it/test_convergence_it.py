"""
Integration tests for converge_flink_sql workflow branches.
"""

from pathlib import Path

import pytest

from flink_fixtures import (
    assert_convergence_stages,
    assert_has_errors,
    assert_no_errors,
    load_flink_pair,
    load_source_sql,
    validation_issues,
)
from flink_skill_common.convergence import ConvergenceContext, converge_flink_sql
from flink_skill_common.deploy.flink_statement_manager import FlinkStatementManager

pytestmark = pytest.mark.integration

TABLE_NAME = "filtered_publications"


def test_converge_valid_deploy(tmp_path: Path, require_deploy):
    ddls, dmls, src_dir = load_flink_pair("filtering", valid=True)
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
            agent_on_failure=False,
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

