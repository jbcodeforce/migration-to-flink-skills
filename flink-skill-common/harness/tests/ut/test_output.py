"""Unit tests for migration output parsing."""

from pathlib import Path

from flink_skill_common.config import HarnessContext, configure, flink_skill_common_skill_dir, skill_dir
__COMMON_ROOT = Path(__file__).resolve().parents[2]
__PROJECT_ROOT = __COMMON_ROOT.parent
configure(HarnessContext(harness_root=__COMMON_ROOT, project_root=__PROJECT_ROOT))


from flink_skill_common.output import (
    extract_sql_blocks,
    parse_source_ddls_from_response,
    resolve_table_paths,
    write_output,
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
