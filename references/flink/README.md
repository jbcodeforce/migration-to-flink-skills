# Flink SQL reference fixtures

Canonical SQL fixtures for `flink-skill-common` integration tests (offline sqlglot + remote CC Flink validation).

## Layout

| Path | Purpose | Expected validation tier |
|------|---------|------------------------|
| `valid/raw_classical_songs/` | Working DDL + DML for table `raw_classical_songs` | offline pass, remote pass |
| `valid/watermark_metadata/` | DDL with WATERMARK + METADATA columns | offline pass |
| `invalid/ddl_bad_syntax/` | Unclosed `CREATE TABLE` paren | offline fail |
| `invalid/ddl_missing_pk/` | DDL without PRIMARY KEY / DISTRIBUTED BY | offline may warn; remote fail |
| `invalid/dml_bad_syntax/` | `INSRT INTO` typo | offline fail |
| `invalid/ddl_fixable_typo/` | Invalid `value.format` in WITH clause | offline warn only; remote fail; agent-fix IT |

Validation-only tests submit SQL via `FlinkStatementManager.validate_sql` (temporary statement names, deleted after check). They do not leave deployed statements.

## Usage

```python
from flink_fixtures import load_pair

ddls, dmls = load_pair("raw_classical_songs", valid=True)
ddls, dmls = load_pair("ddl_bad_syntax", valid=False)
```

Run IT tests from `flink-skill-common/harness`:

```bash
uv run pytest tests/it/test_validation_it.py -k offline -v
uv run pytest tests/it/test_validation_it.py -k remote -v -m integration
uv run pytest tests/it/test_convergence_it.py -v -m integration_agent
```
