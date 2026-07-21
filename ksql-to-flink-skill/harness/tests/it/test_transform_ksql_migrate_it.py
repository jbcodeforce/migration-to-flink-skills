"""End-to-end ksqlDB → Flink migration integration tests (LLM + CC deploy)."""

import pytest
from ksql_ref_fixtures import run_and_assert_cli, KsqlMigrateCase
from flink_skill_common.config import llm_reachable

pytestmark = pytest.mark.integration

def test_llm_reachable():
    assert llm_reachable() 


def test_migrate_ksql_col_diff( require_llm, require_deploy):
    case = KsqlMigrateCase("transformations/col_diff.ksql", "customer_purchases", "transformations")
    run_and_assert_cli(case)

