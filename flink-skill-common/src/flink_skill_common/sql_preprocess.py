"""Shared SQL preprocessing primitives for migration harnesses."""

from __future__ import annotations

import re
from typing import List


def strip_sql_comments_and_drops(sql: str, *, strip_set_statements: bool = False) -> str:
    """Strip comments, DROP TABLE/STREAM, and optionally SET statements."""
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
