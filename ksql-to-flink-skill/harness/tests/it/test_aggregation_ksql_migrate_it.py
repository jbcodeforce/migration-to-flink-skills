"""End-to-end ksqlDB → Flink migration integration tests (LLM + CC deploy)."""

import pytest
from ksql_ref_fixtures import run_and_assert_cli, KsqlMigrateCase

pytestmark = pytest.mark.integration

def test_migrate_ksql_aggregating_count( require_llm, require_deploy):
    case = KsqlMigrateCase("aggregations/aggregating-count.ksql", "movie_count", "aggregation")
    run_and_assert_cli(case)

def test_migrate_ksql_aggregating_sum(require_llm, require_deploy):
    case = KsqlMigrateCase("aggregations/aggregating-sum.ksql", "movie_sum", "aggregation")
    run_and_assert_cli(case)

def test_migrate_ksql_aggregating_minmax(require_llm, require_deploy):
    case = KsqlMigrateCase("aggregations/aggregating-minmax.ksql", "movie_minmax", "aggregation")
    run_and_assert_cli(case)

def test_migrate_ksql_count_pageview(require_llm, require_deploy):
    case = KsqlMigrateCase("aggregations/count_pageviews.ksql", "pageview_counts", "aggregation")
    run_and_assert_cli(case)