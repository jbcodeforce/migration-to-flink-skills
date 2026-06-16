"""Offline ksqlDB utility tests (no LLM)."""

from ksql_flink_skill.sql_utils import clean_ksql_input, split_ksql_create_statements


def test_clean_ksql_removes_set_and_comments():
    sql = """
-- comment
SET 'auto.offset.reset'='earliest';
CREATE STREAM s (id INT) WITH (KAFKA_TOPIC='t');
"""
    cleaned = clean_ksql_input(sql)
    assert "SET " not in cleaned
    assert "comment" not in cleaned
    assert "CREATE STREAM" in cleaned


def test_split_create_stream_statements():
    sql = """
CREATE STREAM a (id INT) WITH (KAFKA_TOPIC='a');
CREATE STREAM b (id INT) WITH (KAFKA_TOPIC='b');
"""
    parts = split_ksql_create_statements(sql)
    assert len(parts) == 2


def test_split_create_table_statements():
    sql = "CREATE TABLE t (k INT PRIMARY KEY) WITH (KAFKA_TOPIC='t');"
    parts = split_ksql_create_statements(sql)
    assert len(parts) == 1
