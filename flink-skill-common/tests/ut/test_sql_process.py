import re

from flink_skill_common.sql_preprocess import split_create_statements, strip_sql_comments_and_drops
from flink_skill_common.output import extract_sql_blocks

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

def test_extract_sql_blocks():
    resp = """
    **Key translations applied:**
- `CREATE STREAM` → `CREATE TABLE IF NOT EXISTS`
- Source `all_publications` uses topic `publication_events` → Flink table `publication_events`
- Target `george_martin` uses topic `george_martin_books` → Flink table `george_martin_books`
- `VARCHAR` → `STRING`
- `EMIT CHANGES` → `INSERT INTO`

```sql
-- DDL for george_martin
CREATE TABLE IF NOT EXISTS publication_events (
    bookid BIGINT,
    author STRING,
    title STRING
) DISTRIBUTED BY HASH(bookid) INTO 1 BUCKETS
WITH (
    'connector' = 'kafka',
    'topic' = 'publication_events',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json-registry',
    'json-registry.schema-context' = '.flink-dev',
    'scan.bounded.mode' = 'unbounded'
);

CREATE TABLE IF NOT EXISTS george_martin_books (
    bookid BIGINT,
    author STRING,
    title STRING,
    PRIMARY KEY (bookid) NOT ENFORCED
) DISTRIBUTED BY HASH(bookid) INTO 1 BUCKETS
WITH (
    'connector' = 'kafka',
    'topic' = 'george_martin_books',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json-registry',
    'json-registry.schema-context' = '.flink-dev',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded'
);
```

```sql
-- DML for george_martin
INSERT INTO george_martin_books
SELECT
    bookid,
    author,
    title
FROM publication_events
WHERE author = 'George R. R. Martin';
```
    """
    ddls, dmls = extract_sql_blocks(resp)
    print(f"DDLs: {ddls}")
    print(f"DMLs: {dmls}")
    assert len(ddls) == 2
    assert any("bookid BIGINT," in s for s in ddls)
    assert len(dmls) == 1
    assert "WHERE author = 'George R. R. Martin'" in dmls[0]