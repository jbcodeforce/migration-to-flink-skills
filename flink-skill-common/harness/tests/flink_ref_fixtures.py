"""Shared helpers for references/flink SQL fixtures (UT + IT)."""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
FLINK_REF = REPO_ROOT / "references" / "flink"
FLINK_VALID_REF = FLINK_REF / "valid"

_CREATE_TABLE = re.compile(r"^\s*CREATE\s+TABLE\b", re.IGNORECASE)
_INSERT_INTO = re.compile(r"^\s*INSERT\s+INTO\b", re.IGNORECASE)


def _classify_fixture(path: Path, sql: str) -> str:
    """Classify fixture SQL as ddl or dml; content wins over filename prefix."""
    stripped = sql.strip()
    if _CREATE_TABLE.match(stripped):
        return "ddl"
    if _INSERT_INTO.match(stripped):
        return "dml"

    name = path.name
    if name.startswith("ddl"):
        return "ddl"
    if name.startswith("dml") or name.startswith("insert"):
        return "dml"
    raise ValueError(f"Unrecognized fixture SQL file: {path.relative_to(FLINK_VALID_REF)}")


def load_all_valid_flink_reference_sql() -> tuple[list[str], list[str]]:
    """Load every *.sql under references/flink/valid, split into DDL and DML lists."""
    if not FLINK_VALID_REF.is_dir():
        raise FileNotFoundError(f"Missing fixture root: {FLINK_VALID_REF}")

    ddls: list[str] = []
    dmls: list[str] = []
    for path in sorted(FLINK_VALID_REF.rglob("*.sql")):
        sql = path.read_text()
        kind = _classify_fixture(path, sql)
        if kind == "ddl":
            ddls.append(sql)
        else:
            dmls.append(sql)

    if not ddls:
        raise FileNotFoundError(f"No DDL fixtures found under {FLINK_VALID_REF}")

    return ddls, dmls
