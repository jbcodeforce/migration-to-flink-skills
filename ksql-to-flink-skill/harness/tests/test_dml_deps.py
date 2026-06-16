"""Tests for DML dependency extraction."""

from ksql_flink_skill.sql_utils import (
    compute_missing_source_tables,
    extract_cte_names,
    extract_created_table_names,
    extract_dml_source_tables,
)


def test_extract_cte_names():
    dml = """
    INSERT INTO target
    WITH deduplicated AS (
        SELECT * FROM source_st
    )
  SELECT * FROM deduplicated GROUP BY id;
    """
    assert extract_cte_names(dml) == ["deduplicated"]


def test_extract_created_table_names():
    ddl = """
    CREATE TABLE IF NOT EXISTS kes_ice_chat_deal (
        id STRING,
        PRIMARY KEY (id) NOT ENFORCED
    );
    """
    assert extract_created_table_names(ddl) == ["kes_ice_chat_deal"]


def test_extract_dml_source_tables_skips_ctes():
    dml = """
    INSERT INTO kes_ice_chat_deal
    WITH deduplicated AS (
        SELECT * FROM kes_ice_chat_deal_st
    )
    SELECT * FROM deduplicated GROUP BY id;
    """
    refs = extract_dml_source_tables(dml, "kes_ice_chat_deal")
    assert refs == ["kes_ice_chat_deal_st"]


def test_extract_dml_source_tables_join():
    dml = """
    INSERT INTO target
    SELECT a.id FROM left_tbl a JOIN right_tbl b ON a.id = b.id;
    """
    refs = extract_dml_source_tables(dml, "target")
    assert refs == ["left_tbl", "right_tbl"]


def test_compute_missing_source_tables():
    dml = """
    INSERT INTO kes_ice_chat_deal
    WITH deduplicated AS (SELECT * FROM kes_ice_chat_deal_st)
    SELECT * FROM deduplicated;
    """
    ddl = "CREATE TABLE IF NOT EXISTS kes_ice_chat_deal (id STRING);"
    missing = compute_missing_source_tables(dml, "kes_ice_chat_deal", ddl)
    assert missing == ["kes_ice_chat_deal_st"]
