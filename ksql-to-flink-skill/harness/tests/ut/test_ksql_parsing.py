"""Offline ksqlDB utility tests (no LLM)."""

from pathlib import Path
from ksql_to_flink.ksql_utils import (
    clean_ksql_input,
    extract_ksql_object_name,
    split_ksql_create_statements,
)
_HARNESS_ROOT = Path(__file__).resolve().parents[2]
_PROJECT_ROOT = _HARNESS_ROOT.parent
from flink_skill_common.config import HarnessContext, configure
configure(HarnessContext(harness_root=_HARNESS_ROOT, project_root=_PROJECT_ROOT))


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
CREATE OR   REPLACE STREAM c (id INT) WITH (  kafka_topic = 'ksql.c', partitions = 3 ) 
AS SELECT id,
LATEST_BY_OFFSET(status) AS status
FROM a GROUP BY id;
"""
    parts = split_ksql_create_statements(sql)
    print(parts)
    assert len(parts) == 3



def test_split_create_table_statements():
    sql = """
CREATE TABLE t (k INT PRIMARY KEY) WITH (KAFKA_TOPIC='t');
CREATE OR REPLACE TABLE c (id INT) WITH (  kafka_topic = 'ksql.c', partitions = 3 ) 
AS SELECT id,
LATEST_BY_OFFSET(status) AS status
FROM a GROUP BY id;
"""

    parts = split_ksql_create_statements(sql)
    assert len(parts) == 2
    print(parts)


def test_extract_ksql_object_name():
    assert extract_ksql_object_name("CREATE STREAM foo (id INT) WITH (KAFKA_TOPIC='f');") == "foo"
    assert extract_ksql_object_name("CREATE OR REPLACE TABLE bar (id INT);") == "bar"
    assert extract_ksql_object_name("SELECT 1;") is None
