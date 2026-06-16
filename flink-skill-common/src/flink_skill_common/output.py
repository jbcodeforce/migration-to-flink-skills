"""Parse agent migration output and write DDL/DML files."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import List, Tuple


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
    return _normalize_sql(ddl), _normalize_sql(dml)


def _extract_sequential_sql_blocks(response: str) -> tuple[str, str]:
    """Use first two ```sql blocks as DDL then DML when unlabeled."""
    blocks = re.findall(r"```(?:sql)?\s*\n(.*?)```", response, re.DOTALL | re.IGNORECASE)
    ddl = blocks[0].strip() if len(blocks) >= 1 else ""
    dml = blocks[1].strip() if len(blocks) >= 2 else ""
    return ddl, dml


def _normalize_sql(sql: str) -> str:
    """Normalize escaped newlines in LLM output."""
    if not sql:
        return sql
    cleaned = sql.replace("\\\\n", "\n").replace("\\n", "\n")
    return cleaned.replace("\\", " ")


def extract_sql_blocks(response: str) -> tuple[str, str]:
    """Parse agent response into DDL and DML SQL strings."""
    if not response or not response.strip():
        return "", ""

    ddl, dml = _extract_labeled_sql_blocks(response)
    if ddl or dml:
        return _normalize_sql(ddl), _normalize_sql(dml)

    ddl, dml = _extract_json_migration(response)
    if ddl or dml:
        return ddl, dml

    ddl, dml = _extract_sequential_sql_blocks(response)
    return _normalize_sql(ddl), _normalize_sql(dml)


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


def write_output(
    table_name: str,
    ddl_statements: List[str],
    dml_statements: List[str],
    out_dir: Path | str,
) -> Tuple[Path, Path]:
    """Write ddl.{table}.sql and dml.{table}.sql to out_dir."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    ddl_path = out / f"ddl.{table_name}.sql"
    dml_path = out / f"dml.{table_name}.sql"
    ddl_path.write_text("\n\n".join(s for s in ddl_statements if s and s.strip()))
    dml_path.write_text("\n\n".join(s for s in dml_statements if s and s.strip()))
    return ddl_path, dml_path


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
