"""Shared helpers for references/flink SQL fixtures (UT + IT)."""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
FLINK_REF = REPO_ROOT / "references" / "flink"
FLINK_VALID_REF = FLINK_REF / "valid"


def load_all_valid_flink_reference_sql() -> tuple[list[str], list[str]]:
    """Load every *.sql under references/flink/valid, split into DDL and DML lists."""
    if not FLINK_VALID_REF.is_dir():
        raise FileNotFoundError(f"Missing fixture root: {FLINK_VALID_REF}")

    ddls: list[str] = []
    dmls: list[str] = []
    for path in sorted(FLINK_VALID_REF.rglob("*.sql")):
        sql = path.read_text()
        name = path.name
        if name.startswith("ddl"):
            ddls.append(sql)
        elif name.startswith("dml") or name.startswith("insert"):
            dmls.append(sql)
        else:
            raise ValueError(f"Unrecognized fixture SQL file: {path.relative_to(FLINK_VALID_REF)}")

    if not ddls:
        raise FileNotFoundError(f"No DDL fixtures found under {FLINK_VALID_REF}")

    return ddls, dmls
