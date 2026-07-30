"""Live LLM integration tests against c360 golden references."""

from pathlib import Path

import pytest

from spark_ref_fixtures import c360_golden_pairs
from spark_to_flink.compare import compare_files_unordered
from spark_to_flink.migrate_agent import run_migration
from spark_to_flink.output import extract_sql_blocks, resolve_table_paths, write_output
from spark_to_flink.sql_utils import clean_sql_input, detect_tables

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("pair_name", ["src_customers"])
def test_live_migration_matches_golden(pair_name: str, require_llm, tmp_path: Path):
    pairs = c360_golden_pairs()
    try:
        pair = next(p for p in pairs if p.name == pair_name)
    except StopIteration:
        pytest.skip(f"No golden pair named {pair_name}")
    if not pair.source_file.exists():
        pytest.skip(f"Missing golden source: {pair.source_file}")

    cleaned = clean_sql_input(pair.source_file.read_text())
    detection = detect_tables(cleaned)
    statements = detection.table_statements if detection.has_multiple_tables else [cleaned]

    ddls: list[str] = []
    dmls: list[str] = []
    for stmt in statements:
        response = run_migration(
            pair.table_name, stmt, cleaned, src_file=pair.source_file
        )
        stmt_ddls, stmt_dmls = extract_sql_blocks(response)
        ddls.extend(stmt_ddls)
        dmls.extend(stmt_dmls)

    ddl_paths, dml_paths = write_output(pair.table_name, ddls, dmls, tmp_path)
    ddl_path, dml_path = resolve_table_paths(ddl_paths, dml_paths, pair.table_name)
    assert ddl_path is not None
    assert dml_path is not None

    ddl_cmp = compare_files_unordered(pair.flink_ddl, ddl_path)
    dml_cmp = compare_files_unordered(pair.flink_dml, dml_path)
    assert ddl_cmp["match_percentage"] >= 80.0, ddl_cmp
    assert dml_cmp["match_percentage"] >= 80.0, dml_cmp
