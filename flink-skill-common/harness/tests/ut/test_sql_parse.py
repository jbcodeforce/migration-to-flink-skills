from pathlib import Path

from flink_skill_common.config import HarnessContext, configure
from flink_skill_common.sql_parse import (
    CREATE_TABLE_SPLIT_PATTERN,
    compute_missing_source_tables,
    extract_created_table_names,
    extract_cte_names,
    extract_ddl_table_name,
    extract_dml_source_tables,
    extract_dml_table_name,
    extract_statement_table_name,
    is_create_table_statement,
    is_insert_into_statement,
    split_create_statements,
    split_ddl_statements,
    split_dml_statements,
    strip_sql_comments_and_drops,
)
from flink_skill_common.response_io import extract_sql_blocks

__COMMON_ROOT = Path(__file__).resolve().parents[2]
__PROJECT_ROOT = __COMMON_ROOT.parent
configure(HarnessContext(harness_root=__COMMON_ROOT, project_root=__PROJECT_ROOT))

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


def test_strip_inline_block_comment_keeps_select():
    sql = """INSERT INTO orders_enriched
SELECT /* STATE_TTL('orders'='1d') */
    orders.customer_id AS customer_id
FROM orders;"""
    cleaned = strip_sql_comments_and_drops(sql)
    assert "STATE_TTL" not in cleaned
    assert "SELECT" in cleaned
    assert "orders.customer_id" in cleaned


def test_strip_set_statements():
    sql = "SET 'auto.offset.reset'='earliest';\nCREATE STREAM s (id STRING);"
    without_set = strip_sql_comments_and_drops(sql, strip_set_statements=False)
    with_set = strip_sql_comments_and_drops(sql, strip_set_statements=True)
    assert "SET" in without_set
    assert "SET" not in with_set


def test_split_create_statements():
    sql = "CREATE TABLE a (id STRING); CREATE TABLE b (id STRING);"
    parts = split_create_statements(sql, CREATE_TABLE_SPLIT_PATTERN)
    assert len(parts) == 2


def test_split_ddl_and_dml_statements():
    ddl = "CREATE TABLE a (id STRING); CREATE TABLE b (id STRING);"
    dml = "INSERT INTO a SELECT 1; INSERT INTO b SELECT 2;"
    assert len(split_ddl_statements(ddl)) == 2
    assert len(split_dml_statements(dml)) == 2


def test_extract_ddl_and_dml_table_names():
    ddl = "CREATE TABLE IF NOT EXISTS `my_table` (id INT);"
    dml = "INSERT INTO `target_table` SELECT id FROM src;"
    assert extract_ddl_table_name(ddl) == "my_table"
    assert extract_dml_table_name(dml) == "target_table"
    assert extract_ddl_table_name("CREATE OR REPLACE TABLE foo (id INT);") == "foo"
    assert extract_statement_table_name(ddl) == "my_table"
    assert extract_statement_table_name(dml) == "target_table"


def test_statement_kind_helpers():
    assert is_create_table_statement("CREATE TABLE t (id INT);")
    assert is_create_table_statement("  CREATE OR REPLACE TABLE t (id INT);")
    assert not is_create_table_statement("INSERT INTO t SELECT 1;")
    assert is_insert_into_statement("INSERT INTO t SELECT 1;")
    assert not is_insert_into_statement("CREATE TABLE t (id INT);")


def test_extract_cte_names():
    dml = """
    WITH ranked AS (
        SELECT id FROM src
    ),
    filtered AS (
        SELECT id FROM ranked
    )
    INSERT INTO target SELECT id FROM filtered;
    """
    assert extract_cte_names(dml) == ["ranked", "filtered"]
    assert extract_cte_names("") == []
    assert extract_cte_names("   ") == []


def test_extract_created_table_names():
    ddl = """
    CREATE TABLE IF NOT EXISTS publication_events (id BIGINT);
    CREATE OR REPLACE TABLE george_martin_books (id BIGINT);
    """
    assert extract_created_table_names(ddl) == [
        "publication_events",
        "george_martin_books",
    ]
    assert extract_created_table_names("") == []


def test_extract_dml_source_tables():
    dml = """
    INSERT INTO george_martin_books
    SELECT a.id
    FROM publication_events a
    JOIN `other_authors` o ON a.author = o.name;
    """
    assert extract_dml_source_tables(dml, "george_martin_books") == [
        "other_authors",
        "publication_events",
    ]


def test_extract_dml_source_tables_excludes_ctes_and_target():
    dml = """
    WITH staged AS (SELECT id FROM src)
    INSERT INTO target SELECT id FROM staged JOIN dim ON staged.id = dim.id;
    """
    assert extract_dml_source_tables(dml, "target") == ["dim", "src"]


def test_extract_dml_source_tables_empty():
    assert extract_dml_source_tables("", "target") == []


def test_compute_missing_source_tables():
    dml = """
    INSERT INTO george_martin_books
    SELECT bookid FROM publication_events;
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS george_martin_books (bookid BIGINT);
    """
    assert compute_missing_source_tables(dml, "george_martin_books", ddl) == [
        "publication_events",
    ]


def test_compute_missing_source_tables_none_when_defined_in_ddl():
    dml = "INSERT INTO target SELECT id FROM src;"
    ddl = """
    CREATE TABLE IF NOT EXISTS target (id BIGINT);
    CREATE TABLE IF NOT EXISTS src (id BIGINT);
    """
    assert compute_missing_source_tables(dml, "target", ddl) == []


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