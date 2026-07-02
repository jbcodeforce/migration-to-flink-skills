"""Unit tests for migration output parsing."""

from pathlib import Path

from flink_skill_common.config import HarnessContext, configure, flink_skill_common_skill_dir, skill_dir
__COMMON_ROOT = Path(__file__).resolve().parents[2]
__PROJECT_ROOT = __COMMON_ROOT.parent
configure(HarnessContext(harness_root=__COMMON_ROOT, project_root=__PROJECT_ROOT))


from flink_skill_common.response_io import (
    _disambiguated_stem,
    _normalize_sql,
    _split_statements,
    extract_sql_blocks,
    parse_source_ddls_from_response,
    resolve_table_paths,
    strip_markdown_fence,
    write_output,
    write_source_ddls,
)
from flink_skill_common.sql_parse import extract_ddl_table_name, extract_dml_table_name


def test_strip_markdown_fence():
    assert strip_markdown_fence("plain sql") == "plain sql"
    assert strip_markdown_fence("```sql\nCREATE TABLE t (id INT);\n```") == "CREATE TABLE t (id INT);"
    assert strip_markdown_fence("```json") == "json"
    assert strip_markdown_fence("```sql\nSELECT 1;\n```", lang="sql") == "SELECT 1;"
    assert strip_markdown_fence("") == ""
    assert strip_markdown_fence("   ") == "   "


def test_normalize_sql():
    assert _normalize_sql("") == ""
    assert _normalize_sql("line1\\nline2") == "line1\nline2"
    assert _normalize_sql("line1\\\\nline2") == "line1\nline2"
    assert _normalize_sql("foo\\bar") == "foo bar"


def test_split_statements():
    ddl = "CREATE TABLE a (id INT); CREATE TABLE b (id INT);"
    dml = "INSERT INTO a SELECT 1; INSERT INTO b SELECT 2;"
    ddls, dmls = _split_statements(ddl, dml)
    assert len(ddls) == 2
    assert "CREATE TABLE a" in ddls[0]
    assert "CREATE TABLE b" in ddls[1]
    assert len(dmls) == 2
    assert "INSERT INTO a" in dmls[0]
    assert "INSERT INTO b" in dmls[1]


def test_extract_ddl_and_dml_table_names():
    ddl = "CREATE TABLE IF NOT EXISTS `my_table` (id INT);"
    dml = "INSERT INTO `target_table` SELECT id FROM src;"
    assert extract_ddl_table_name(ddl) == "my_table"
    assert extract_dml_table_name(dml) == "target_table"
    assert extract_ddl_table_name("SELECT 1") is None
    assert extract_dml_table_name("SELECT 1") is None


def test_disambiguated_stem():
    assert _disambiguated_stem("ddl", "foo", 0, 1) == "ddl.foo"
    assert _disambiguated_stem("ddl", "foo", 0, 2) == "ddl.foo_0"
    assert _disambiguated_stem("ddl", "foo", 1, 2) == "ddl.foo_1"


def test_extract_sql_blocks_empty():
    assert extract_sql_blocks("") == ([], [])
    assert extract_sql_blocks("   ") == ([], [])


def test_extract_sql_blocks_normalizes_json_newlines():
    response = """```json
{
  "flink_ddl_output": "CREATE TABLE IF NOT EXISTS t (id STRING);",
  "flink_dml_output": "INSERT INTO t\\nSELECT id FROM src;"
}
```"""
    ddls, dmls = extract_sql_blocks(response)
    assert ddls and "CREATE TABLE" in ddls[0]
    assert dmls and "INSERT INTO t\nSELECT" in dmls[0]


def test_extract_sql_blocks_splits_multiple_statements():
    response = """
DDL:
```sql
CREATE TABLE IF NOT EXISTS a (id INT);
CREATE TABLE IF NOT EXISTS b (id INT);
```

DML:
```sql
INSERT INTO a SELECT 1;
INSERT INTO b SELECT 2;
```
"""
    ddls, dmls = extract_sql_blocks(response)
    assert len(ddls) == 2
    assert len(dmls) == 2


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
    ddls, dmls = extract_sql_blocks(response)
    print(f"DDLs: {ddls}")
    print(f"DMLs: {dmls}")
    assert ddls and "CREATE TABLE" in ddls[0]
    assert dmls and "INSERT INTO" in dmls[0]

def test_extract_labeled_sql_blocks_without_columns():
    response = """
    ```sql
DDL:
CREATE TABLE IF NOT EXISTS george_martin (
    -- columns inferred from SELECT * on all_publications
    -- define explicit column types once all_publications schema is available
);
```

```sql
DML:
INSERT INTO george_martin SELECT * FROM all_publications WHERE author = 'George R. R. Martin';
```
"""
    ddls, dmls = extract_sql_blocks(response)
    print(f"DDLs: {ddls}")
    print(f"DMLs: {dmls}")
    assert ddls and "CREATE TABLE" in ddls[0]
    assert dmls and "INSERT INTO" in dmls[0]

def test_extract_json_migration():
    response = """```json
{
  "flink_ddl_output": "CREATE TABLE IF NOT EXISTS t (id STRING);",
  "flink_dml_output": "INSERT INTO t SELECT id FROM src;"
}
```"""
    ddls, dmls = extract_sql_blocks(response)
    assert ddls and "CREATE TABLE" in ddls[0]
    assert dmls and "INSERT INTO" in dmls[0]


def test_extract_sequential_sql_blocks():
    response = """
```sql
CREATE TABLE IF NOT EXISTS t (id STRING);
```

```sql
INSERT INTO t SELECT id FROM src;
```
"""
    ddls, dmls = extract_sql_blocks(response)
    assert ddls and "CREATE TABLE" in ddls[0]
    assert dmls and "INSERT INTO" in dmls[0]


def test_parse_source_ddls_from_response():
    response = """{
  "source_ddls": [
    {"table": "src_st", "ddl": "CREATE TABLE IF NOT EXISTS src_st (id STRING);"}
  ]
}"""
    parsed = parse_source_ddls_from_response(response)
    assert "src_st" in parsed
    assert "CREATE TABLE" in parsed["src_st"]


def test_parse_source_ddls_invalid_json():
    assert parse_source_ddls_from_response("not json") == {}
    assert parse_source_ddls_from_response('{"other": []}') == {}


def test_parse_source_ddls_normalizes_escaped_newlines():
    response = """{
  "source_ddls": [
    {"table": "src_st", "ddl": "CREATE TABLE IF NOT EXISTS src_st (id STRING);\\n"}
  ]
}"""
    parsed = parse_source_ddls_from_response(response)
    assert parsed["src_st"].endswith(");")


def test_write_output_one_file_per_statement(tmp_path: Path):
    out = tmp_path / "output"
    ddls = [
        "CREATE TABLE IF NOT EXISTS publication_events (bookid BIGINT);",
        "CREATE TABLE IF NOT EXISTS george_martin_books (bookid BIGINT);",
    ]
    dmls = [
        "INSERT INTO george_martin_books SELECT bookid FROM publication_events;",
    ]
    ddl_paths, dml_paths = write_output("fallback", ddls, dmls, out)

    assert len(ddl_paths) == 2
    assert {p.name for p in ddl_paths} == {
        "ddl.publication_events.sql",
        "ddl.george_martin_books.sql",
    }
    assert len(dml_paths) == 1
    assert dml_paths[0].name == "dml.george_martin_books.sql"
    assert ddl_paths[0].read_text().startswith("CREATE TABLE")


def test_write_output_duplicate_table_suffix(tmp_path: Path):
    out = tmp_path / "output"
    ddls = [
        "CREATE TABLE IF NOT EXISTS foo (id INT);",
        "CREATE TABLE IF NOT EXISTS foo (name STRING);",
    ]
    ddl_paths, _ = write_output("fallback", ddls, [], out)

    assert len(ddl_paths) == 2
    assert {p.name for p in ddl_paths} == {"ddl.foo_0.sql", "ddl.foo_1.sql"}


def test_write_output_uses_fallback_name(tmp_path: Path):
    out = tmp_path / "output"
    ddl_paths, dml_paths = write_output(
        "fallback_table",
        ["SELECT 1;"],
        ["SELECT 2;"],
        out,
    )
    assert ddl_paths[0].name == "ddl.fallback_table.sql"
    assert dml_paths[0].name == "dml.fallback_table.sql"


def test_resolve_table_paths(tmp_path: Path):
    out = tmp_path / "output"
    ddls = [
        "CREATE TABLE IF NOT EXISTS src (id INT);",
        "CREATE TABLE IF NOT EXISTS target (id INT);",
    ]
    dmls = ["INSERT INTO target SELECT id FROM src;"]
    ddl_paths, dml_paths = write_output("fallback", ddls, dmls, out)

    ddl_path, dml_path = resolve_table_paths(ddl_paths, dml_paths, "target")
    assert ddl_path is not None
    assert ddl_path.name == "ddl.target.sql"
    assert dml_path is not None
    assert dml_path.name == "dml.target.sql"


def test_resolve_table_paths_disambiguated_suffix(tmp_path: Path):
    out = tmp_path / "output"
    ddls = [
        "CREATE TABLE IF NOT EXISTS target (id INT);",
        "CREATE TABLE IF NOT EXISTS target (name STRING);",
    ]
    dmls = [
        "INSERT INTO target SELECT id FROM src;",
        "INSERT INTO target SELECT name FROM src;",
    ]
    ddl_paths, dml_paths = write_output("fallback", ddls, dmls, out)

    ddl_path, dml_path = resolve_table_paths(ddl_paths, dml_paths, "target")
    assert ddl_path is not None
    assert ddl_path.name == "ddl.target_0.sql"
    assert dml_path is not None
    assert dml_path.name == "dml.target_0.sql"


def test_write_source_ddls_layout(tmp_path: Path):
    out = tmp_path / "output"
    paths = write_source_ddls(
        out,
        {
            "kma_chat_deal_st": "CREATE TABLE IF NOT EXISTS kma_chat_deal_st (id STRING);",
        },
    )
    assert len(paths) == 1
    assert paths[0].name == "ddl.kma_chat_deal_st.sql"
    assert paths[0].parent.name == "tests"
    assert paths[0].read_text().startswith("CREATE TABLE")
