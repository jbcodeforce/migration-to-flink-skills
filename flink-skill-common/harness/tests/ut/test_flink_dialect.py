"""Unit tests for the custom Flink sqlglot dialect."""

import pytest
import sqlglot
from sqlglot.errors import ParseError

from flink_skill_common.sqlglot_flink import Flink, MetadataColumnConstraint, Watermark


def test_flink_dialect_registered():
    from sqlglot.dialects.dialect import Dialect

    assert Dialect.get("flink") is Flink


def test_parse_watermark_alone():
    sql = (
        "CREATE TABLE t (ts TIMESTAMP_LTZ(3), "
        "WATERMARK FOR ts AS ts - INTERVAL '5' SECOND)"
    )
    ast = sqlglot.parse_one(sql, read="flink")
    watermarks = list(ast.find_all(Watermark))
    assert len(watermarks) == 1
    column = watermarks[0].args["column"]
    assert column.this.name == "ts"  # type: ignore[union-attr]


def test_parse_watermark_after_primary_key():
    sql = """CREATE TABLE t (
        bookid BIGINT,
        ts TIMESTAMP_LTZ(3),
        PRIMARY KEY (bookid) NOT ENFORCED,
        WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
    )"""
    ast = sqlglot.parse_one(sql, read="flink")
    assert list(ast.find_all(Watermark))
    assert list(ast.find_all(sqlglot.exp.PrimaryKey))


def test_parse_metadata_from():
    sql = """CREATE TABLE t (
        event_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
    )"""
    ast = sqlglot.parse_one(sql, read="flink")
    metadata = list(ast.find_all(MetadataColumnConstraint))
    assert len(metadata) == 1
    from_key = metadata[0].args["from_key"]
    assert from_key.this == "timestamp"  # type: ignore[union-attr]
    assert metadata[0].args.get("virtual") is False


def test_parse_metadata_virtual_with_backticks():
    sql = """CREATE TABLE t (
        `partition` BIGINT METADATA VIRTUAL,
        `offset` BIGINT METADATA VIRTUAL
    )"""
    ast = sqlglot.parse_one(sql, read="flink")
    metadata = list(ast.find_all(MetadataColumnConstraint))
    assert len(metadata) == 2
    assert all(m.args.get("virtual") for m in metadata)


def test_parse_combined_flink_ddl():
    sql = """CREATE TABLE t (
        `user_id` BIGINT,
        `event_time` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
        `partition` BIGINT METADATA VIRTUAL,
        PRIMARY KEY (user_id) NOT ENFORCED,
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    )"""
    ast = sqlglot.parse_one(sql, read="flink")
    assert list(ast.find_all(Watermark))
    assert len(list(ast.find_all(MetadataColumnConstraint))) == 2


def test_malformed_watermark_raises():
    sql = "CREATE TABLE t (WATERMARK FOR ts AS)"
    with pytest.raises(ParseError):
        sqlglot.parse_one(sql, read="flink")
