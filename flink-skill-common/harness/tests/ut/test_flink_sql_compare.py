"""Unit tests for Flink golden SQL comparison helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from flink_skill_common.flink_sql_compare import (
    FLINK_VALID_ROOT,
    assert_pipeline_matches_reference,
    assert_structure_matches,
    compare_sql_files,
    extract_primary_key,
    iter_reference_sql_files,
)

REF_CUSTOMERS = (
    FLINK_VALID_ROOT / "joins" / "multi-joins" / "customers" / "ddl.customers.sql"
)


@dataclass(frozen=True)
class _Case:
    rel_path: str
    category: str


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def test_extract_primary_key_order_and_case():
    sql = """
    CREATE TABLE t (
      A STRING,
      b STRING,
      PRIMARY KEY (A, b) NOT ENFORCED
    );
    """
    assert extract_primary_key(sql) == ("a", "b")


def test_extract_primary_key_absent():
    assert extract_primary_key("CREATE TABLE t (id STRING);") is None


def test_iter_reference_sql_files_skips_tests():
    ref = FLINK_VALID_ROOT / "joins" / "multi-joins"
    rels = iter_reference_sql_files(ref)
    names = {str(r) for r in rels}
    assert "customers/ddl.customers.sql" in names
    assert "orders_enriched/dml.orders_enriched.sql" in names
    assert not any("tests" in str(r) for r in rels)


def test_assert_structure_matches_passes(tmp_path: Path):
    ref = tmp_path / "ref"
    out = tmp_path / "out"
    _write(ref / "t" / "ddl.t.sql", "CREATE TABLE t (id STRING);")
    _write(ref / "t" / "dml.t.sql", "INSERT INTO t SELECT 1;")
    _write(ref / "t" / "tests" / "insert.sql", "-- ignored")
    _write(out / "t" / "ddl.t.sql", "CREATE TABLE t (id STRING);")
    _write(out / "t" / "dml.t.sql", "INSERT INTO t SELECT 1;")
    assert_structure_matches(ref, out)


def test_assert_structure_matches_missing_file(tmp_path: Path):
    ref = tmp_path / "ref"
    out = tmp_path / "out"
    _write(ref / "t" / "ddl.t.sql", "CREATE TABLE t (id STRING);")
    _write(ref / "t" / "dml.t.sql", "INSERT INTO t SELECT 1;")
    _write(out / "t" / "ddl.t.sql", "CREATE TABLE t (id STRING);")
    with pytest.raises(AssertionError, match="Missing generated SQL"):
        assert_structure_matches(ref, out)


def test_compare_ignores_serde_format_lines(tmp_path: Path):
    ref = _write(
        tmp_path / "ref.sql",
        """CREATE TABLE IF NOT EXISTS customers (
    customer_id STRING,
    customer_name STRING,
    PRIMARY KEY (customer_id) NOT ENFORCED
) DISTRIBUTED BY HASH(customer_id) INTO 6 BUCKETS
WITH (
    'key.format' = 'avro-registry',
    'value.format' = 'avro-registry',
    'scan.startup.mode' = 'earliest-offset'
);
""",
    )
    created = _write(
        tmp_path / "created.sql",
        """CREATE TABLE IF NOT EXISTS customers (
    customer_id STRING,
    customer_name STRING,
    PRIMARY KEY (customer_id) NOT ENFORCED
) DISTRIBUTED BY HASH(customer_id) INTO 6 BUCKETS
WITH (
    'value.format' = 'json-registry',
    'scan.startup.mode' = 'earliest-offset'
);
""",
    )
    cmp = compare_sql_files(ref, created)
    assert cmp["primary_key_match"] is True
    assert cmp["match_percentage"] >= 80.0
    assert cmp["reference_pk"] == ("customer_id",)


def test_compare_strict_primary_key_mismatch(tmp_path: Path):
    ref = _write(
        tmp_path / "ref.sql",
        """CREATE TABLE t (
    a STRING, b STRING,
    PRIMARY KEY (a, b) NOT ENFORCED
);
""",
    )
    created = _write(
        tmp_path / "created.sql",
        """CREATE TABLE t (
    a STRING, b STRING,
    PRIMARY KEY (a) NOT ENFORCED
);
""",
    )
    cmp = compare_sql_files(ref, created)
    assert cmp["primary_key_match"] is False
    assert cmp["reference_pk"] == ("a", "b")
    assert cmp["created_pk"] == ("a",)


def test_compare_self_match_real_reference():
    assert REF_CUSTOMERS.is_file()
    cmp = compare_sql_files(REF_CUSTOMERS, REF_CUSTOMERS)
    assert cmp["primary_key_match"] is True
    assert cmp["match_percentage"] == 100.0
    assert cmp["reference_pk"] == ("customer_id",)


def test_assert_pipeline_matches_reference_pk_failure(tmp_path: Path):
    case = _Case("joins/toy.ksql", "joins")
    ref = tmp_path / "ref"
    out = tmp_path / "out"
    _write(
        ref / "t" / "ddl.t.sql",
        """CREATE TABLE t (
    id STRING,
    PRIMARY KEY (id) NOT ENFORCED
);
""",
    )
    _write(
        out / "t" / "ddl.t.sql",
        """CREATE TABLE t (
    id STRING,
    PRIMARY KEY (other) NOT ENFORCED
);
""",
    )
    with pytest.raises(AssertionError, match="PRIMARY KEY mismatch"):
        assert_pipeline_matches_reference(case, out, ref_root=ref)


def test_assert_pipeline_matches_reference_end_to_end(tmp_path: Path):
    case = _Case("joins/toy.ksql", "joins")
    ref = tmp_path / "flink_valid" / "joins" / "toy"
    out = tmp_path / "out"
    sql = """CREATE TABLE t (
    id STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'value.format' = 'avro-registry'
);
"""
    created = """CREATE TABLE t (
    id STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'value.format' = 'json-registry'
);
"""
    _write(ref / "t" / "ddl.t.sql", sql)
    _write(out / "t" / "ddl.t.sql", created)
    assert_pipeline_matches_reference(case, out, ref_root=ref)
