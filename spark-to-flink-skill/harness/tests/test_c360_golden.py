"""Live LLM integration tests against c360 golden references."""

from pathlib import Path

import pytest

from spark_flink_skill.agents.migrate_agent import run_migration
from spark_flink_skill.compare import compare_files_unordered
from spark_flink_skill.fixtures import c360_golden_pairs
from spark_flink_skill.output import extract_sql_blocks, write_output
from spark_flink_skill.sql_utils import clean_sql_input, detect_tables, llm_reachable


pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def require_llm():
    if not llm_reachable():
        pytest.skip("LLM not reachable at SL_LLM_BASE_URL")


@pytest.mark.parametrize("pair_name", ["src_customers"])
def test_live_migration_matches_golden(pair_name: str, require_llm, tmp_path: Path):
    pair = next(p for p in c360_golden_pairs() if p.name == pair_name)
    cleaned = clean_sql_input(pair.source_file.read_text())
    detection = detect_tables(cleaned)
    statements = detection.table_statements if detection.has_multiple_tables else [cleaned]

    ddls: list[str] = []
    dmls: list[str] = []
    for stmt in statements:
        response = run_migration(pair.table_name, stmt)
        ddl, dml = extract_sql_blocks(response)
        if ddl.strip():
            ddls.append(ddl)
        if dml.strip():
            dmls.append(dml)

    ddl_path, dml_path = write_output(pair.table_name, ddls, dmls, tmp_path)

    ddl_cmp = compare_files_unordered(pair.flink_ddl, ddl_path)
    dml_cmp = compare_files_unordered(pair.flink_dml, dml_path)
    assert ddl_cmp["match_percentage"] >= 80.0, ddl_cmp
    assert dml_cmp["match_percentage"] >= 80.0, dml_cmp
