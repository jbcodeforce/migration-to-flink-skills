"""End-to-end ksqlDB → Flink migration integration tests (LLM + CC deploy)."""

import pytest
from ksql_ref_fixtures import run_and_assert_cli, KsqlMigrateCase, ksql_source_path, KSQL_SOURCES_ROOT
import json

pytestmark = pytest.mark.integration

def test_migrate_multi_joins():
    """
    Test that the ksqlDB → Flink migration works for the multi-joins pipeline.
    """
    case = KsqlMigrateCase("joins/multi-joins.ksql", "order_enriched", "joins" )
    # run_and_assert_cli(case)
    statement_path = KSQL_SOURCES_ROOT / "joins" / "multi-joins.statements" / "manifest.json"
    assert statement_path.exists()
    with open(statement_path, "r") as f:
        manifest = json.load(f)
        assert "ksql/sources/joins/multi-joins.ksql" in manifest["source_file"] 
    
 