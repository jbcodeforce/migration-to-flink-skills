"""
Copyright 2024-2026 Confluent, Inc.
KSQL to Flink SQL Translation Agent

Parse agent migration output and write DDL/DML files.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import List, Tuple

from flink_skill_common.config import get_logger
from flink_skill_common.sql_preprocess import split_create_statements

_CREATE_TABLE_PATTERN = re.compile(r"\bCREATE\s+TABLE\b", re.IGNORECASE)
_INSERT_INTO_PATTERN = re.compile(r"\bINSERT\s+INTO\b", re.IGNORECASE)
_CREATE_TABLE_NAME_PATTERN = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?([a-zA-Z_][a-zA-Z0-9_]*)`?",
    re.IGNORECASE,
)
_INSERT_INTO_TABLE_PATTERN = re.compile(
    r"INSERT\s+INTO\s+`?([a-zA-Z_][a-zA-Z0-9_]*)`?",
    re.IGNORECASE,
)

def strip_markdown_fence(text: str, lang: str | None = None) -> str:
    """Remove markdown code fences from a block."""
    if not text or not text.strip():
        return text
    s = text.strip()
    if not s.startswith("```"):
        return s
    first_newline = s.find("\n")
    if first_newline != -1:
        s = s[first_newline + 1 :]
    else:
        s = s[3:]
        if lang and s.lower().startswith(lang.lower()):
            s = s[len(lang) :]
        s = s.lstrip()
    if s.endswith("```"):
        s = s[:-3].rstrip()
    return s


def _extract_labeled_sql_blocks(response: str) -> tuple[str, str]:
    """Extract DDL and DML from labeled ```sql blocks in agent markdown."""
    ddl = ""
    dml = ""
    pattern = re.compile(
        r"(?:^|\n)\s*(?:#+\s*)?(DDL|DML)\s*:?\s*\n```(?:sql)?\s*\n(.*?)```",
        re.IGNORECASE | re.DOTALL,
    )
    for label, body in pattern.findall(response):
        sql = body.strip()
        if label.upper() == "DDL":
            ddl = sql
        elif label.upper() == "DML":
            dml = sql
    get_logger().info("DDL: %s", ddl)
    get_logger().info("DML: %s", dml)
    return ddl, dml


def _extract_json_migration(response: str) -> tuple[str, str]:
    """Extract DDL/DML from JSON output per skill format."""
    text = response.strip()
    if "```" in text:
        match = re.search(r"```(?:json)?\s*\n(.*?)```", text, re.DOTALL | re.IGNORECASE)
        if match:
            text = match.group(1).strip()
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return "", ""
    ddl = (data.get("flink_ddl_output") or "").strip()
    dml = (data.get("flink_dml_output") or "").strip()
    get_logger().info("DDL: %s", ddl)
    get_logger().info("DML: %s", dml)
    return _normalize_sql(ddl), _normalize_sql(dml)


def _extract_sequential_sql_blocks(response: str) -> tuple[str, str]:
    """Use first two ```sql blocks as DDL then DML when unlabeled."""
    blocks = re.findall(r"```(?:sql)?\s*\n(.*?)```", response, re.DOTALL | re.IGNORECASE)
    ddl = blocks[0].strip() if len(blocks) >= 1 else ""
    dml = blocks[1].strip() if len(blocks) >= 2 else ""
    get_logger().info("DDL: %s", ddl)
    get_logger().info("DML: %s", dml)
    return ddl, dml


def _normalize_sql(sql: str) -> str:
    """Normalize escaped newlines in LLM output."""
    if not sql:
        return sql
    cleaned = sql.replace("\\\\n", "\n").replace("\\n", "\n")
    return cleaned.replace("\\", " ")


def _split_statements(ddl: str, dml: str) -> tuple[list[str], list[str]]:
    """Split extracted DDL/DML blobs into individual statements."""
    ddls = split_create_statements(ddl, _CREATE_TABLE_PATTERN)
    dmls = split_create_statements(dml, _INSERT_INTO_PATTERN)
    return ddls, dmls


def extract_sql_blocks(response: str) -> tuple[list[str], list[str]]:
    """Parse agent response into lists of DDL and DML statements."""
    if not response or not response.strip():
        return [], []

    ddl, dml = _extract_labeled_sql_blocks(response)
    if ddl or dml:
        return _split_statements(_normalize_sql(ddl), _normalize_sql(dml))

    ddl, dml = _extract_json_migration(response)
    if ddl or dml:
        return _split_statements(ddl, dml)

    ddl, dml = _extract_sequential_sql_blocks(response)
    return _split_statements(_normalize_sql(ddl), _normalize_sql(dml))


def parse_source_ddls_from_response(response: str) -> dict[str, str]:
    """Parse source DDL JSON from LLM response into table -> ddl mapping."""
    text = response.strip()
    if "```" in text:
        match = re.search(r"```(?:json)?\s*\n(.*?)```", text, re.DOTALL | re.IGNORECASE)
        if match:
            text = match.group(1).strip()
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return {}

    items = data.get("source_ddls") if isinstance(data, dict) else None
    if not isinstance(items, list):
        return {}

    result: dict[str, str] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        table = (item.get("table") or "").strip()
        ddl = _normalize_sql((item.get("ddl") or "").strip())
        if table and ddl:
            result[table] = ddl
    return result


def _extract_ddl_table_name(statement: str) -> str | None:
    """Return the table name from a CREATE TABLE statement."""
    match = _CREATE_TABLE_NAME_PATTERN.search(statement)
    return match.group(1) if match else None


def _extract_dml_table_name(statement: str) -> str | None:
    """Return the target table name from an INSERT INTO statement."""
    match = _INSERT_INTO_TABLE_PATTERN.search(statement)
    return match.group(1) if match else None


def _disambiguated_stem(prefix: str, table_name: str, index: int, total: int) -> str:
    """Build ddl.{table} or ddl.{table}_{index} when duplicate names exist."""
    if total > 1:
        return f"{prefix}.{table_name}_{index}"
    return f"{prefix}.{table_name}"


def _write_sql_files(
    statements: List[str],
    out_dir: Path,
    prefix: str,
    name_extractor,
    fallback_name: str,
) -> List[Path]:
    """Write one file per statement, named by extracted table with duplicate suffixes."""
    logger = get_logger()
    entries: list[tuple[str, str, int]] = []
    name_totals: dict[str, int] = {}
    name_seen: dict[str, int] = {}

    for statement in statements:
        if not statement or not statement.strip():
            continue
        sql = statement.strip()
        table_name = name_extractor(sql) or fallback_name
        index = name_seen.get(table_name, 0)
        name_seen[table_name] = index + 1
        name_totals[table_name] = name_seen[table_name]
        entries.append((table_name, sql, index))

    warned_duplicates: set[str] = set()
    paths: List[Path] = []
    for table_name, sql, index in entries:
        total = name_totals[table_name]
        if total > 1 and table_name not in warned_duplicates:
            logger.warning(
                "Duplicate %s table %r (%d statements): writing files with _N suffix",
                prefix,
                table_name,
                total,
            )
            warned_duplicates.add(table_name)
        stem = _disambiguated_stem(prefix, table_name, index, total)
        path = out_dir / f"{stem}.sql"
        path.write_text(sql)
        paths.append(path)
    return paths


def resolve_table_paths(
    ddl_paths: List[Path],
    dml_paths: List[Path],
    table_name: str,
) -> Tuple[Path | None, Path | None]:
    """Return ddl/dml paths for table_name, matching exact or _N disambiguated names."""
    table_lower = table_name.lower()

    def _match(paths: List[Path], prefix: str) -> Path | None:
        exact = f"{prefix}.{table_name}.sql"
        for path in paths:
            if path.name.lower() == exact.lower():
                return path
        prefixed = f"{prefix}.{table_name}_"
        for path in paths:
            name_lower = path.name.lower()
            if name_lower.startswith(prefixed.lower()) and name_lower.endswith(".sql"):
                suffix = name_lower[len(prefixed) : -4]
                if suffix.isdigit():
                    return path
        for path in paths:
            stem = path.stem.lower()
            if stem == f"{prefix}.{table_lower}" or stem.startswith(f"{prefix}.{table_lower}_"):
                return path
        return paths[0] if paths else None

    return _match(ddl_paths, "ddl"), _match(dml_paths, "dml")


def write_output(
    table_name: str,
    ddl_statements: List[str],
    dml_statements: List[str],
    out_dir: Path | str,
) -> Tuple[List[Path], List[Path]]:
    """Write ddl.{table}.sql and dml.{table}.sql files, one per statement."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    ddl_paths = _write_sql_files(
        ddl_statements,
        out,
        "ddl",
        _extract_ddl_table_name,
        table_name,
    )
    dml_paths = _write_sql_files(
        dml_statements,
        out,
        "dml",
        _extract_dml_table_name,
        table_name,
    )
    return ddl_paths, dml_paths


def write_source_ddls(out_dir: Path | str, source_ddls: dict[str, str]) -> List[Path]:
    """Write tests/ddl.{source}.sql for each source table stub."""
    out = Path(out_dir)
    tests_dir = out / "tests"
    tests_dir.mkdir(parents=True, exist_ok=True)
    paths: List[Path] = []
    for table_name in sorted(source_ddls.keys(), key=str.lower):
        ddl = source_ddls[table_name].strip()
        if not ddl:
            continue
        path = tests_dir / f"ddl.{table_name}.sql"
        path.write_text(ddl)
        paths.append(path)
    return paths
