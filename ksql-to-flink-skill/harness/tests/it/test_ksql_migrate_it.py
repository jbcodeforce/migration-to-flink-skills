"""End-to-end ksqlDB → Flink migration integration tests (LLM + CC deploy)."""

import pytest

from ksql_ref_fixtures import KSQL_MIGRATE_CASES, ksql_source_path, staging_out_dir
from ksql_to_flink.cli import app

from live_cli_runner import LiveCliRunner

pytestmark = pytest.mark.integration

runner = LiveCliRunner()


@pytest.mark.parametrize("case", KSQL_MIGRATE_CASES, ids=lambda c: c.rel_path)
def test_migrate_ksql_source(case, require_llm, require_deploy, tmp_path):
    out_dir = staging_out_dir(tmp_path, case)
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
    assert result.exit_code == 0, result.output
