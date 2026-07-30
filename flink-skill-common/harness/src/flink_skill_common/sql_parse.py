"""
Copyright 2024-2026 Confluent, Inc.
KSQL to Flink SQL Translation Agent

Flink SQL parsing primitives: stripping, splitting, and dependency analysis.
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

# Pattern to extract table names from FROM or JOIN clauses
_TABLE_REF_PATTERN = re.compile(
    r"\b(?:FROM|JOIN)\s+(?:`([^`]+)`|([a-zA-Z_][a-zA-Z0-9_]*))",
    re.IGNORECASE,
)
# Pattern to extract CTE names from WITH clause
_CTE_NAME_PATTERN = re.compile(
    r"(?:\bWITH|,)\s*([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s*\(",
    re.IGNORECASE,
)

# Split boundaries (find statement starts in a SQL blob)
CREATE_TABLE_SPLIT_PATTERN = re.compile(r"\bCREATE\s+TABLE\b", re.IGNORECASE)
INSERT_INTO_SPLIT_PATTERN = re.compile(r"\bINSERT\s+INTO\b", re.IGNORECASE)

# Table name extraction (first match in a statement)
_CREATE_TABLE_NAME_PATTERN = re.compile(
    r"CREATE(?:\s+OR\s+REPLACE)?\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?([a-zA-Z_][a-zA-Z0-9_]*)`?",
    re.IGNORECASE,
)
_INSERT_INTO_NAME_PATTERN = re.compile(
    r"INSERT\s+INTO\s+`?([a-zA-Z_][a-zA-Z0-9_]*)`?",
    re.IGNORECASE,
)

# Statement kind (start-anchored, for validation and fixture classification)
_CREATE_TABLE_START_PATTERN = re.compile(
    r"^\s*CREATE(?:\s+OR\s+REPLACE)?\s+TABLE\b",
    re.IGNORECASE,
)
_INSERT_INTO_START_PATTERN = re.compile(
    r"^\s*INSERT\s+INTO\b",
    re.IGNORECASE,
)

# DDL table-property WITH clause (not DML CTE WITH ... AS)
_DDL_WITH_ANCHOR = re.compile(r"\bWITH\s*\(", re.IGNORECASE)

# 'key' = 'value' or "key" = "value"
_QUOTED_PROPERTY_PATTERN = re.compile(
    r"""['"]([^'"]+)['"]\s*=\s*['"]([^'"]*)['"]""",
)

# unquoted_key = 'value' (e.g. kafka.topic = 't')
_UNQUOTED_KEY_PROPERTY_PATTERN = re.compile(
    r"""([a-zA-Z][a-zA-Z0-9_.-]*)\s*=\s*['"]([^'"]*)['"]""",
)


def extract_ddl_table_name(statement: str) -> str | None:
    """Return the table name from a CREATE TABLE statement."""
    match = _CREATE_TABLE_NAME_PATTERN.search(statement)
    return match.group(1) if match else None


def extract_dml_table_name(statement: str) -> str | None:
    """Return the target table name from an INSERT INTO statement."""
    match = _INSERT_INTO_NAME_PATTERN.search(statement)
    return match.group(1) if match else None


def extract_statement_table_name(sql: str) -> str | None:
    """Return the table name from a CREATE TABLE or INSERT INTO statement."""
    name = extract_ddl_table_name(sql)
    if name:
        return name
    return extract_dml_table_name(sql)


def is_create_table_statement(sql: str) -> bool:
    """True when sql starts with CREATE [OR REPLACE] TABLE."""
    return bool(_CREATE_TABLE_START_PATTERN.match(sql.strip()))


def is_insert_into_statement(sql: str) -> bool:
    """True when sql starts with INSERT INTO."""
    return bool(_INSERT_INTO_START_PATTERN.match(sql.strip()))


def split_ddl_statements(sql: str) -> List[str]:
    """Split a DDL blob into individual CREATE TABLE statements."""
    return split_create_statements(sql, CREATE_TABLE_SPLIT_PATTERN)


def split_dml_statements(sql: str) -> List[str]:
    """Split a DML blob into individual INSERT INTO statements."""
    return split_create_statements(sql, INSERT_INTO_SPLIT_PATTERN)


def extract_cte_names(dml_sql: str) -> List[str]:
    """Return CTE names declared in a WITH clause."""
    if not dml_sql or not dml_sql.strip():
        return []
    return list(dict.fromkeys(m.group(1) for m in _CTE_NAME_PATTERN.finditer(dml_sql)))


def extract_created_table_names(ddl_sql: str) -> List[str]:
    """Return table names from CREATE TABLE IF NOT EXISTS statements."""
    if not ddl_sql or not ddl_sql.strip():
        return []
    return list(dict.fromkeys(m.group(1) for m in _CREATE_TABLE_NAME_PATTERN.finditer(ddl_sql)))


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

        if in_block:
            if "*/" not in stripped:
                continue
            stripped = stripped.split("*/", 1)[1].strip()
            in_block = False
            if not stripped:
                continue

        while "/*" in stripped:
            before, rest = stripped.split("/*", 1)
            if "*/" in rest:
                _, after = rest.split("*/", 1)
                stripped = f"{before}{after}".strip()
            else:
                in_block = True
                stripped = before.strip()
                break

        if not stripped:
            continue

        upper = stripped.upper()
        if upper.startswith("DROP TABLE") or upper.startswith("DROP STREAM"):
            continue
        if strip_set_statements and upper.startswith("SET "):
            continue
        cleaned.append(stripped)

    return "\n".join(cleaned)


def split_create_statements(sql: str, create_pattern: re.Pattern[str]) -> List[str]:
    """Split script into statements using a dialect-specific start pattern."""
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


def _line_number_at_offset(sql: str, offset: int) -> int:
    """Return 1-based line number for a character offset in sql."""
    return sql.count("\n", 0, offset) + 1


def _extract_balanced_parens(sql: str, open_paren_index: int) -> str | None:
    """Return inner text between open_paren_index and its matching ')'."""
    if open_paren_index >= len(sql) or sql[open_paren_index] != "(":
        return None
    depth = 0
    for index in range(open_paren_index, len(sql)):
        char = sql[index]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return sql[open_paren_index + 1 : index]
    return None


def extract_ddl_with_block(sql: str) -> tuple[str | None, int | None]:
    """Return (inner text between WITH parens, 1-based line of WITH) or (None, None)."""
    if not sql or not is_create_table_statement(sql):
        return None, None

    match = _DDL_WITH_ANCHOR.search(sql)
    if not match:
        return None, None

    open_paren = sql.find("(", match.end() - 1)
    if open_paren == -1:
        return None, None

    inner = _extract_balanced_parens(sql, open_paren)
    if inner is None:
        return None, None

    return inner, _line_number_at_offset(sql, match.start())


def parse_with_properties(with_inner: str) -> dict[str, tuple[str, int]]:
    """Return {property_key: (value, line)} for key = 'value' pairs in a WITH block."""
    if not with_inner or not with_inner.strip():
        return {}

    properties: dict[str, tuple[str, int]] = {}
    seen_spans: set[tuple[int, int]] = set()

    for match in _QUOTED_PROPERTY_PATTERN.finditer(with_inner):
        key = match.group(1).lower()
        value = match.group(2)
        line = _line_number_at_offset(with_inner, match.start())
        properties[key] = (value, line)
        seen_spans.add(match.span())

    for match in _UNQUOTED_KEY_PROPERTY_PATTERN.finditer(with_inner):
        if any(start <= match.start() < end for start, end in seen_spans):
            continue
        key = match.group(1).lower()
        value = match.group(2)
        line = _line_number_at_offset(with_inner, match.start())
        properties[key] = (value, line)

    return properties


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
