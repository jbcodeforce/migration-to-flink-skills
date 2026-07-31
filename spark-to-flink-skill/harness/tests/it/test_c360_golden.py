"""Live LLM migration IT against references/spark/c360/sources tables."""

import pytest

from spark_ref_fixtures import SparkMigrateCase, run_and_assert_cli

pytestmark = pytest.mark.integration


def test_migrate_spark_src_purchases(require_llm, require_deploy):
    case = SparkMigrateCase(
        "tables/src_purchases.sql", "src_purchases", "seed"
    )
    run_and_assert_cli(case)

def test_migrate_spark_src_web_events(require_llm, require_deploy):
    case = SparkMigrateCase(
        "tables/src_web_events.sql", "src_web_events", "seed"
    )
    run_and_assert_cli(case)
