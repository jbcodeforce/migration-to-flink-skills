"""
Copyright 2024-2026 Confluent, Inc.
KSQL to Flink SQL Translation Agent

Flink statement naming and source DDL discovery.
"""

from __future__ import annotations

import re
from pathlib import Path

STATEMENT_NAME_RE = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_CREATE_TABLE_NAME = re.compile(
    r"^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?([a-zA-Z_][a-zA-Z0-9_]*)`?",
    re.IGNORECASE,
)
_INSERT_INTO_TABLE = re.compile(
    r"^\s*INSERT\s+INTO\s+`?([a-zA-Z_][a-zA-Z0-9_]*)`?",
    re.IGNORECASE,
)



def normalize_statement_prefix(table_name: str) -> str:
    """Normalize table name for Flink statement names (hyphens, lowercase)."""
    normalized = table_name.lower().replace("_", "-")
    if not STATEMENT_NAME_RE.match(normalized):
        raise ValueError(
            f"Table name {table_name!r} cannot be normalized to a valid statement name prefix"
        )
    return normalized


def ddl_statement_name(table_name: str) -> str:
    return f"{normalize_statement_prefix(table_name)}-ddl"


def dml_statement_name(table_name: str) -> str:
    return f"{normalize_statement_prefix(table_name)}-dml"

def _extract_table_name(sql: str) -> str | None:
    """Return the table name from a CREATE TABLE or INSERT INTO statement."""
    match = _CREATE_TABLE_NAME.search(sql)
    if match:
        return match.group(1)
    match = _INSERT_INTO_TABLE.search(sql)
    return match.group(1) if match else None

def discover_source_ddl_files(tests_dir: Path) -> list[tuple[str, Path]]:
    """Return (table_name, path) for each tests/ddl.{table}.sql source stub."""
    if not tests_dir.is_dir():
        return []
    results: list[tuple[str, Path]] = []
    for path in sorted(tests_dir.glob("*.sql")):
        stem = path.stem
        table_name = _extract_table_name(path.read_text())
        if table_name:
            results.append((table_name, path))
    return results
