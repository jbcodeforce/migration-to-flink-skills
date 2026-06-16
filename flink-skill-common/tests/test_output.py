"""Unit tests for migration output parsing."""

from pathlib import Path

from flink_skill_common.output import (
    extract_sql_blocks,
    parse_source_ddls_from_response,
    write_source_ddls,
)


def test_extract_labeled_sql_blocks():
    response = """
DDL:
```sql
CREATE TABLE IF NOT EXISTS t (id STRING, PRIMARY KEY (id) NOT ENFORCED);
```

DML:
```sql
INSERT INTO t SELECT id FROM src;
```
"""
    ddl, dml = extract_sql_blocks(response)
    assert "CREATE TABLE" in ddl
    assert "INSERT INTO" in dml


def test_extract_json_migration():
    response = """```json
{
  "flink_ddl_output": "CREATE TABLE IF NOT EXISTS t (id STRING);",
  "flink_dml_output": "INSERT INTO t SELECT id FROM src;"
}
```"""
    ddl, dml = extract_sql_blocks(response)
    assert "CREATE TABLE" in ddl
    assert "INSERT INTO" in dml


def test_extract_sequential_sql_blocks():
    response = """
```sql
CREATE TABLE IF NOT EXISTS t (id STRING);
```

```sql
INSERT INTO t SELECT id FROM src;
```
"""
    ddl, dml = extract_sql_blocks(response)
    assert "CREATE TABLE" in ddl
    assert "INSERT INTO" in dml


def test_parse_source_ddls_from_response():
    response = """{
  "source_ddls": [
    {"table": "src_st", "ddl": "CREATE TABLE IF NOT EXISTS src_st (id STRING);"}
  ]
}"""
    parsed = parse_source_ddls_from_response(response)
    assert "src_st" in parsed
    assert "CREATE TABLE" in parsed["src_st"]


def test_write_source_ddls_layout(tmp_path: Path):
    out = tmp_path / "output"
    paths = write_source_ddls(
        out,
        {
            "kes_ice_chat_deal_st": "CREATE TABLE IF NOT EXISTS kes_ice_chat_deal_st (id STRING);",
        },
    )
    assert len(paths) == 1
    assert paths[0].name == "ddl.kes_ice_chat_deal_st.sql"
    assert paths[0].parent.name == "tests"
    assert paths[0].read_text().startswith("CREATE TABLE")
