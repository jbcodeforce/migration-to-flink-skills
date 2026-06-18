"""Live LLM integration tests against ksql golden references."""

from pathlib import Path

import pytest

from ksql_flink_skill.agents.migrate_agent import run_migration
from ksql_flink_skill.compare import compare_files_unordered
from ksql_flink_skill.fixtures import ksql_golden_pairs
from ksql_flink_skill.output import extract_sql_blocks, write_output
from ksql_flink_skill.sql_utils import clean_ksql_input, llm_reachable

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def require_llm():
    if not llm_reachable():
        pytest.skip("LLM not reachable at SL_LLM_BASE_URL")


@pytest.mark.parametrize("pair_name", ["merge"])
def test_live_migration_matches_golden(pair_name: str, require_llm, tmp_path: Path):
    pair = next(p for p in ksql_golden_pairs() if p.name == pair_name)
    cleaned = clean_ksql_input(pair.source_file.read_text())
    response = run_migration(pair.table_name, cleaned)
    ddls, dmls = extract_sql_blocks(response)
    ddl_paths, dml_paths = write_output(pair.table_name, ddls, dmls, tmp_path)
    ddl_path, dml_path = resolve_table_paths(ddl_paths, dml_paths, pair.table_name)
    assert ddl_path is not None
    assert dml_path is not None

    ddl_cmp = compare_files_unordered(pair.flink_ddl, ddl_path)
    dml_cmp = compare_files_unordered(pair.flink_dml, dml_path)
    assert ddl_cmp["match_percentage"] >= 80.0, ddl_cmp
    assert dml_cmp["match_percentage"] >= 80.0, dml_cmp
