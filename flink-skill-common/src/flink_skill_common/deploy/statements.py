"""Flink statement naming and source DDL discovery."""

from __future__ import annotations

import re
from pathlib import Path

STATEMENT_NAME_RE = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")


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


def discover_source_ddl_files(tests_dir: Path) -> list[tuple[str, Path]]:
    """Return (table_name, path) for each tests/ddl.{table}.sql source stub."""
    if not tests_dir.is_dir():
        return []
    results: list[tuple[str, Path]] = []
    for path in sorted(tests_dir.glob("ddl.*.sql")):
        stem = path.stem
        if stem.startswith("ddl."):
            table_name = stem[4:]
            if table_name:
                results.append((table_name, path))
    return results
