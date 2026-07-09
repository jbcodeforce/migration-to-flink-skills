"""End-to-end ksqlDB → Flink migration integration tests (LLM + CC deploy)."""

import pytest
from ksql_ref_fixtures import run_and_assert_cli, KsqlMigrateCase

pytestmark = pytest.mark.integration

def test_migrate_ksql_filtering( require_llm, require_deploy):
    case = KsqlMigrateCase("routing/filtering.ksql", "george_martin", "routing")
    run_and_assert_cli(case)

def test_migrate_ksql_deduplication( require_llm, require_deploy):
    case = KsqlMigrateCase("routing/deduplicate.ksql", "detected_clicks", "routing")
    run_and_assert_cli(case)

def test_migrate_ksql_merge( require_llm, require_deploy):
    case = KsqlMigrateCase("routing/merge.ksql", "all_songs", "routing")
    run_and_assert_cli(case)

def test_migrate_ksql_splitting( require_llm, require_deploy):
    case = KsqlMigrateCase("routing/splitting.ksql", "acting_events_drama", "routing")
    run_and_assert_cli(case)