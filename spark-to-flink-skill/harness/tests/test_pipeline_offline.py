"""Offline SQL utility tests (no LLM)."""

from spark_flink_skill.sql_utils import (
    clean_sql_input,
    detect_tables,
    split_sql_create_statements,
)


def test_clean_sql_removes_comments_and_drop():
    sql = """
-- comment
DROP TABLE IF EXISTS old;
SELECT 1;
"""
    cleaned = clean_sql_input(sql)
    assert "DROP TABLE" not in cleaned
    assert "comment" not in cleaned
    assert "SELECT 1" in cleaned


def test_split_create_statements():
    sql = """
CREATE TABLE a (id INT);
CREATE TABLE b (id INT);
"""
    parts = split_sql_create_statements(sql)
    assert len(parts) == 2


def test_detect_single_script_without_create():
    sql = "INSERT INTO t SELECT 1;"
    det = detect_tables(sql)
    assert det.has_multiple_tables is False
    assert len(det.table_statements) == 1


def test_detect_spark_temporary_views():
    sql = """
CREATE OR REPLACE TEMPORARY VIEW raw AS SELECT 1;
CREATE OR REPLACE TEMPORARY VIEW src AS SELECT 2;
"""
    det = detect_tables(sql)
    assert det.has_multiple_tables is True
    assert len(det.table_statements) == 2
