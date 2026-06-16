import re

from flink_skill_common.sql_preprocess import split_create_statements, strip_sql_comments_and_drops


def test_strip_sql_comments_and_drops():
    sql = """
-- comment
DROP TABLE foo;
CREATE TABLE t (id STRING);
"""
    cleaned = strip_sql_comments_and_drops(sql)
    assert "DROP TABLE" not in cleaned
    assert "CREATE TABLE" in cleaned
    assert "-- comment" not in cleaned


def test_strip_set_statements():
    sql = "SET 'auto.offset.reset'='earliest';\nCREATE STREAM s (id STRING);"
    without_set = strip_sql_comments_and_drops(sql, strip_set_statements=False)
    with_set = strip_sql_comments_and_drops(sql, strip_set_statements=True)
    assert "SET" in without_set
    assert "SET" not in with_set


def test_split_create_statements():
    sql = "CREATE TABLE a (id STRING); CREATE TABLE b (id STRING);"
    pattern = re.compile(r"\bCREATE\s+TABLE\b", re.IGNORECASE)
    parts = split_create_statements(sql, pattern)
    assert len(parts) == 2
