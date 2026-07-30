"""End-to-end ksqlDB → Flink migration integration tests (LLM + CC deploy)."""
import os
import pytest
from flink_skill_common.flink_sql_compare import assert_pipeline_matches_reference
from ksql_ref_fixtures import KsqlMigrateCase, run_and_assert_cli

pytestmark = pytest.mark.integration


def test_migrate_multi_joins(require_llm, require_deploy):
    """
    Migrate multi-joins pipeline and compare against references/flink/valid/joins/multi-joins.
    """
    case = KsqlMigrateCase("joins/multi-joins.ksql", "orders_enriched", "joins")
    out_dir = run_and_assert_cli(case)
    assert_pipeline_matches_reference(case, out_dir)
    


def test_migr_stream_stream_join(require_llm, require_deploy):
    """
    Migrate stream-stream join pipeline and compare against references/flink/valid/joins/stream-stream-join.
    """
    case = KsqlMigrateCase("joins/stream_stream.ksql", "shipped_orders", "joins")
    out_dir = run_and_assert_cli(case)
    print(f"Output directory: {out_dir}")
    assert out_dir is not None
    assert os.path.exists(out_dir)
    