"""End-to-end ksqlDB → Flink migration integration tests (LLM + CC deploy)."""

import pytest

from ksql_ref_fixtures import KSQL_MIGRATE_CASES, ksql_source_path, staging_out_dir, KsqlMigrateCase, REPO_ROOT
from ksql_to_flink.cli import app
from live_cli_runner import LiveCliRunner

pytestmark = pytest.mark.integration

runner = LiveCliRunner()
output_path = REPO_ROOT / "staging" / "ksql2flk"

#@pytest.mark.parametrize("case", KSQL_MIGRATE_CASES, ids=lambda c: c.rel_path)
def test_migrate_ksql_filtering( require_llm, require_deploy):
    case = KsqlMigrateCase("routing/filtering.ksql", "george_martin", "routing")
    out_dir = staging_out_dir(output_path, case)

    result = runner.invoke(
        app,
        [
            "--table",
            case.target_table,
            "--file",
            str(ksql_source_path(case)),
            "--out-dir",
            str(out_dir),
        ],
    )
    print(result.output)
    assert result.exit_code == 0, result.output

def test_migrate_ksql_deduplication( require_llm, require_deploy):
    case = KsqlMigrateCase("routing/deduplicate.ksql", "detected_clicks", "routing")
    out_dir = staging_out_dir(output_path, case)

    result = runner.invoke(
        app,
        [
            "--table",
            case.target_table,
            "--file",
            str(ksql_source_path(case)),
                        "--out-dir",
            str(out_dir),
        ],
    )
    print(result.output)
    assert result.exit_code == 0, result.output

def test_migrate_ksql_merge( require_llm, require_deploy):
    case = KsqlMigrateCase("routing/merge.ksql", "all_songs", "routing")
    out_dir = staging_out_dir(output_path, case)

    result = runner.invoke(
        app,
        [
            "--table",
            case.target_table,
            "--file",
            str(ksql_source_path(case)),
                        "--out-dir",
            str(out_dir),
        ],
    )
    print(result.output)
    assert result.exit_code == 0, result.output

def test_migrate_ksql_splitting( require_llm, require_deploy):
    case = KsqlMigrateCase("routing/splitting.ksql", "acting_events_drama", "routing")
    out_dir = staging_out_dir(output_path, case)

    result = runner.invoke(
        app,
        [
            "--table",
            case.target_table,
            "--file",
            str(ksql_source_path(case)),
            "--out-dir",
            str(out_dir),
        ],
    )
    print(result.output)
    assert result.exit_code == 0, result.output