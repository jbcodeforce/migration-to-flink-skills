"""Unit tests for DDL WITH property extraction and validation rules."""

from flink_skill_common.sql_parse import (
    extract_ddl_with_block,
    parse_with_properties,
)
from flink_skill_common.with_property_rules import validate_with_properties


VALID_WITH_INNER = """
    'changelog.mode' = 'append',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all',
    'value.format' = 'avro-registry'
"""

VALID_DDL = f"""CREATE TABLE IF NOT EXISTS t (
    id BIGINT
) DISTRIBUTED BY HASH(id) INTO 1 BUCKETS WITH ({VALID_WITH_INNER});"""

VALID_DDL_NO_DISTRIBUTED = f"""CREATE TABLE IF NOT EXISTS t (
    id BIGINT
) WITH ({VALID_WITH_INNER});"""


def test_extract_ddl_with_block_with_distributed_by():
    inner, line = extract_ddl_with_block(VALID_DDL)
    assert inner is not None
    assert "'changelog.mode' = 'append'" in inner
    assert line == 3


def test_extract_ddl_with_block_without_distributed_by():
    inner, line = extract_ddl_with_block(VALID_DDL_NO_DISTRIBUTED)
    assert inner is not None
    assert "'value.format' = 'avro-registry'" in inner
    assert line == 3


def test_extract_ddl_with_block_returns_none_for_dml():
    dml = "INSERT INTO t WITH staged AS (SELECT 1) SELECT * FROM staged;"
    assert extract_ddl_with_block(dml) == (None, None)


def test_parse_with_properties_quoted_keys():
    props = parse_with_properties(VALID_WITH_INNER)
    assert props["changelog.mode"] == ("append", 2)
    assert props["value.format"] == ("avro-registry", 8)


def test_parse_with_properties_unquoted_key():
    inner = "kafka.topic = 'my-topic', 'changelog.mode' = 'append'"
    props = parse_with_properties(inner)
    assert props["kafka.topic"] == ("my-topic", 1)
    assert props["changelog.mode"] == ("append", 1)


def test_validate_with_properties_valid_set():
    props = parse_with_properties(VALID_WITH_INNER)
    issues = validate_with_properties(props, statement_index=0)
    errors = [i for i in issues if i.severity == "error"]
    assert not errors


def test_validate_with_properties_invalid_value_format():
    inner = "'value.format' = 'invalid-format-xyz'"
    props = parse_with_properties(inner)
    issues = validate_with_properties(props, statement_index=0)
    errors = [i for i in issues if i.severity == "error"]
    assert len(errors) == 1
    assert "value.format" in errors[0].message
    assert "invalid-format-xyz" in errors[0].message


def test_validate_with_properties_invalid_changelog_mode():
    inner = "'changelog.mode' = 'retract'"
    props = parse_with_properties(inner)
    issues = validate_with_properties(props, statement_index=0)
    errors = [i for i in issues if i.severity == "error"]
    assert len(errors) == 1
    assert "changelog.mode" in errors[0].message


def test_validate_with_properties_warns_deprecated_connector():
    inner = "'connector' = 'kafka', 'changelog.mode' = 'append'"
    props = parse_with_properties(inner)
    issues = validate_with_properties(props, statement_index=0)
    warnings = [i for i in issues if i.severity == "warning"]
    assert any("connector" in i.message for i in warnings)


def test_validate_with_properties_warns_missing_changelog_mode():
    inner = "'value.format' = 'avro-registry'"
    props = parse_with_properties(inner)
    issues = validate_with_properties(props, statement_index=0)
    warnings = [i for i in issues if i.severity == "warning"]
    assert any("changelog.mode" in i.message for i in warnings)


def test_validate_with_properties_invalid_schema_context():
    inner = (
        "'key.avro-registry.schema-context' = 'flink-dev', "
        "'changelog.mode' = 'append'"
    )
    props = parse_with_properties(inner)
    issues = validate_with_properties(props, statement_index=0)
    errors = [i for i in issues if i.severity == "error"]
    assert len(errors) == 1
    assert "schema-context" in errors[0].message


def test_validate_with_properties_valid_schema_context():
    inner = (
        "'key.avro-registry.schema-context' = '.flink-dev', "
        "'changelog.mode' = 'append'"
    )
    props = parse_with_properties(inner)
    issues = validate_with_properties(props, statement_index=0)
    errors = [i for i in issues if i.severity == "error"]
    assert not errors
