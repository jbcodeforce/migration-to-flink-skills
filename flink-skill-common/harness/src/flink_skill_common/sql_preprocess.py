"""
Copyright 2024-2026 Confluent, Inc.
KSQL to Flink SQL Translation Agent

Shared FlinkSQL processing primitives for migration harnesses.

"""

from __future__ import annotations

import re
from typing import List

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

_TABLE_REF_PATTERN = re.compile(
    r"\b(?:FROM|JOIN)\s+(?:`([^`]+)`|([a-zA-Z_][a-zA-Z0-9_]*))",
    re.IGNORECASE,
)

_CTE_NAME_PATTERN = re.compile(
    r"(?:\bWITH|,)\s*([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s*\(",
    re.IGNORECASE,
)

_CREATE_TABLE_PATTERN = re.compile(
    r"CREATE(?:\s+OR\s+REPLACE)?\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?([a-zA-Z_][a-zA-Z0-9_]*)`?",
    re.IGNORECASE,
)



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



def strip_sql_comments_and_drops(sql: str, *, strip_set_statements: bool = False) -> str:
    """Strip comments, DROP TABLE, and optionally SET statements."""
    lines = sql.split("\n")
    cleaned: List[str] = []
    in_block = False

    for line in lines:
        stripped = line.strip()
        if not stripped:
            cleaned.append(stripped)
            continue
        if stripped.startswith("--"):
            continue
        if "/*" in stripped and "*/" in stripped:
            continue
        if "/*" in stripped:
            in_block = True
            continue
        if "*/" in stripped:
            in_block = False
            continue
        if in_block:
            continue
        upper = stripped.upper()
        if upper.startswith("DROP TABLE") or upper.startswith("DROP STREAM"):
            continue
        if strip_set_statements and upper.startswith("SET "):
            continue
        cleaned.append(stripped)

    return "\n".join(cleaned)


def split_create_statements(sql: str, create_pattern: re.Pattern[str]) -> List[str]:
    """Split script into CREATE statements using a dialect-specific pattern."""
    if not sql or not sql.strip():
        return []
    starts = [m.start() for m in create_pattern.finditer(sql)]
    if not starts:
        return []
    statements: List[str] = []
    for i, start in enumerate(starts):
        next_create = starts[i + 1] if i + 1 < len(starts) else len(sql)
        semi = sql.find(";", start)
        end = semi + 1 if semi != -1 and semi < next_create else next_create
        stmt = sql[start:end].strip()
        if stmt:
            statements.append(stmt)
    return statements


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


