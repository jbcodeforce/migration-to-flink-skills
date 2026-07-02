"""
Copyright 2024-2026 Confluent, Inc.

Offline validation rules for Flink DDL WITH-clause connector properties.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flink_skill_common.sql_validate import SqlKind, SqlValidationIssue

_ENUM_RULES: dict[str, frozenset[str]] = {
    "changelog.mode": frozenset({"append", "upsert"}),
    "value.format": frozenset({"json-registry", "avro-registry"}),
    "key.format": frozenset({"json-registry", "avro-registry"}),
    "scan.startup.mode": frozenset({"earliest-offset"}),
    "scan.bounded.mode": frozenset({"unbounded"}),
    "value.fields-include": frozenset({"all"}),
    "kafka.producer.compression.type": frozenset({"snappy"}),
}

_SCHEMA_CONTEXT_SUFFIX = ".schema-context"
_SCHEMA_CONTEXT_PATTERN = re.compile(r"^\..+")

_DEPRECATED_KEYS = frozenset({"connector", "topic", "format"})

_RECOMMENDED_KEYS = frozenset({"changelog.mode"})


@dataclass(frozen=True)
class _PropertyRule:
    allowed_values: frozenset[str] | None = None
    value_pattern: re.Pattern[str] | None = None


def _rule_for_key(key: str) -> _PropertyRule | None:
    if key in _ENUM_RULES:
        return _PropertyRule(allowed_values=_ENUM_RULES[key])
    if key.endswith(_SCHEMA_CONTEXT_SUFFIX):
        return _PropertyRule(value_pattern=_SCHEMA_CONTEXT_PATTERN)
    return None


def validate_with_properties(
    properties: dict[str, tuple[str, int]],
    *,
    statement_index: int,
    kind: "SqlKind" = "ddl",
) -> list["SqlValidationIssue"]:
    """Validate extracted WITH properties against skill-standard rules."""
    from flink_skill_common.sql_validate import SqlValidationIssue

    issues: list[SqlValidationIssue] = []

    for key in _RECOMMENDED_KEYS:
        if key not in properties:
            issues.append(
                SqlValidationIssue(
                    statement_index=statement_index,
                    kind=kind,
                    message=f"DDL WITH clause missing '{key}' property",
                    severity="warning",
                )
            )

    for key, (value, line) in properties.items():
        if key in _DEPRECATED_KEYS:
            issues.append(
                SqlValidationIssue(
                    statement_index=statement_index,
                    kind=kind,
                    message=f"Deprecated WITH property '{key}' should be removed",
                    line=line,
                    severity="warning",
                )
            )
            continue

        rule = _rule_for_key(key)
        if rule is None:
            continue

        if rule.allowed_values is not None and value not in rule.allowed_values:
            allowed = ", ".join(sorted(rule.allowed_values))
            issues.append(
                SqlValidationIssue(
                    statement_index=statement_index,
                    kind=kind,
                    message=(
                        f"Invalid value '{value}' for WITH property '{key}'; "
                        f"expected one of: {allowed}"
                    ),
                    line=line,
                    severity="error",
                )
            )
        elif rule.value_pattern is not None and not rule.value_pattern.match(value):
            issues.append(
                SqlValidationIssue(
                    statement_index=statement_index,
                    kind=kind,
                    message=(
                        f"Invalid value '{value}' for WITH property '{key}'; "
                        "expected a schema context starting with '.'"
                    ),
                    line=line,
                    severity="error",
                )
            )

    return issues
