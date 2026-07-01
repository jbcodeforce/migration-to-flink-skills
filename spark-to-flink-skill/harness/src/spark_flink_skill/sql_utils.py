"""Deterministic Spark SQL preprocessing utilities."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List

from flink_skill_common.llm import (
    LlmConfigError,
    ensure_model_context,
    fetch_available_models,
    fetch_model_context_windows,
    is_agent_error_response,
    llm_reachable,
    resolve_llm_model,
)
from flink_skill_common.sql_parse import split_create_statements, strip_sql_comments_and_drops

_SPARK_CREATE_PATTERN = re.compile(
    r"\bCREATE\s+(?:TABLE|OR\s+REPLACE\s+TEMPORARY\s+VIEW)\b", re.IGNORECASE
)


@dataclass
class SqlTableDetection:
    has_multiple_tables: bool
    table_statements: List[str]
    description: str


def split_sql_create_statements(sql: str) -> List[str]:
    """Split script into CREATE TABLE statements by boundaries."""
    return split_create_statements(sql, _SPARK_CREATE_PATTERN)


def clean_sql_input(sql: str) -> str:
    """Strip DROP statements and comments from Spark SQL input."""
    return strip_sql_comments_and_drops(sql, strip_set_statements=False)


def detect_tables(sql: str) -> SqlTableDetection:
    """Detect multiple CREATE statements (deterministic, no LLM)."""
    split_list = split_sql_create_statements(sql)
    n = len(split_list)
    if n > 1:
        return SqlTableDetection(
            has_multiple_tables=True,
            table_statements=split_list,
            description=f"Split by CREATE boundaries: {n} statements",
        )
    if n == 1:
        return SqlTableDetection(
            has_multiple_tables=False,
            table_statements=split_list,
            description="Single CREATE statement",
        )
    return SqlTableDetection(
        has_multiple_tables=False,
        table_statements=[sql],
        description="No CREATE statements found, treating as single script",
    )


__all__ = [
    "LlmConfigError",
    "SqlTableDetection",
    "clean_sql_input",
    "detect_tables",
    "ensure_model_context",
    "fetch_available_models",
    "fetch_model_context_windows",
    "is_agent_error_response",
    "llm_reachable",
    "resolve_llm_model",
    "split_sql_create_statements",
]
