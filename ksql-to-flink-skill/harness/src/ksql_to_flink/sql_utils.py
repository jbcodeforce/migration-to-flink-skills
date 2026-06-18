"""Deterministic ksqlDB preprocessing utilities."""

from __future__ import annotations

import re
from typing import List


from flink_skill_common.sql_preprocess import split_create_statements, strip_sql_comments_and_drops

_KSQL_CREATE_PATTERN = re.compile(r"\bCREATE\s+(?:STREAM|TABLE)\b", re.IGNORECASE)

_SQL_KEYWORDS = frozenset(
    {
        "select", "where", "group", "order", "by", "on", "as", "and", "or", "not",
        "null", "inner", "left", "right", "outer", "full", "cross", "lateral", "union",
        "all", "distinct", "limit", "offset", "insert", "into", "values", "set", "with",
        "case", "when", "then", "else", "end", "between", "like", "in", "exists", "having",
        "from", "join", "table", "stream", "primary", "key", "enforced", "distributed",
        "buckets", "if", "not", "exists", "create", "over", "partition", "row_number",
    }
)

_CTE_NAME_PATTERN = re.compile(
    r"(?:\bWITH|,)\s*([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s*\(",
    re.IGNORECASE,
)

_CREATE_TABLE_PATTERN = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?([a-zA-Z_][a-zA-Z0-9_]*)`?",
    re.IGNORECASE,
)

_TABLE_REF_PATTERN = re.compile(
    r"\b(?:FROM|JOIN)\s+(?:`([^`]+)`|([a-zA-Z_][a-zA-Z0-9_]*))",
    re.IGNORECASE,
)


def split_ksql_create_statements(sql: str) -> List[str]:
    """Split ksql script into CREATE STREAM/TABLE statements."""
    return split_create_statements(sql, _KSQL_CREATE_PATTERN)


def clean_ksql_input(sql: str) -> str:
    """Strip DROP, SET, comments from ksqlDB input."""
    return strip_sql_comments_and_drops(sql, strip_set_statements=True)


def extract_cte_names(dml_sql: str) -> List[str]:
    """Return CTE names declared in a WITH clause."""
    if not dml_sql or not dml_sql.strip():
        return []
    return list(dict.fromkeys(m.group(1) for m in _CTE_NAME_PATTERN.finditer(dml_sql)))


def extract_created_table_names(ddl_sql: str) -> List[str]:
    """Return table names from CREATE TABLE IF NOT EXISTS statements."""
    if not ddl_sql or not ddl_sql.strip():
        return []
    return list(dict.fromkeys(m.group(1) for m in _CREATE_TABLE_PATTERN.finditer(ddl_sql)))


def extract_dml_source_tables(dml_sql: str, target_table: str) -> List[str]:
    """Return sorted unique table names referenced via FROM or JOIN in DML."""
    if not dml_sql or not dml_sql.strip():
        return []
    cte_names = {n.lower() for n in extract_cte_names(dml_sql)}
    target_lower = target_table.lower()
    seen: dict[str, str] = {}

    for match in _TABLE_REF_PATTERN.finditer(dml_sql):
        name = match.group(1) or match.group(2)
        if not name:
            continue
        lower = name.lower()
        if lower in _SQL_KEYWORDS or lower in cte_names or lower == target_lower:
            continue
        if lower not in seen:
            seen[lower] = name

    return sorted(seen.values(), key=str.lower)


def compute_missing_source_tables(
    dml_sql: str,
    target_table: str,
    ddl_sql: str,
) -> List[str]:
    """Tables referenced in DML that are not the target and not defined in target DDL."""
    refs = extract_dml_source_tables(dml_sql, target_table)
    created = {n.lower() for n in extract_created_table_names(ddl_sql)}
    target_lower = target_table.lower()
    missing = [
        name
        for name in refs
        if name.lower() not in created and name.lower() != target_lower
    ]
    return missing


