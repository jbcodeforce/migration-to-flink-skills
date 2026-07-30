"""Compare migrated Flink SQL trees against references/flink/valid goldens.

Shared by ksql-to-flink and spark-to-flink harness tests.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from flink_skill_common.config import find_repo_root


@runtime_checkable
class FlinkPipelineCase(Protocol):
    """Minimal case shape: category + source relative path (stem = pipeline dir)."""

    rel_path: str
    category: str


def flink_valid_root(repo_root: Path | None = None) -> Path:
    root = repo_root or find_repo_root()
    return root / "references" / "flink" / "valid"


# Backward-compatible module-level aliases (resolved lazily via properties would
# be nicer, but callers already import these names).
REPO_ROOT = find_repo_root()
FLINK_VALID_ROOT = REPO_ROOT / "references" / "flink" / "valid"

_DDL_DML_RE = re.compile(r"^(ddl|dml)\..+\.sql$", re.IGNORECASE)
_PRIMARY_KEY_RE = re.compile(
    r"PRIMARY\s+KEY\s*\(([^)]+)\)",
    re.IGNORECASE | re.DOTALL,
)
_SERDE_KEYS = ("key.format", "value.format")


def reference_pipeline_dir(
    case: FlinkPipelineCase,
    *,
    flink_valid_root_path: Path | None = None,
) -> Path:
    """Resolve ``references/flink/valid/{category}/{stem}`` for a migrate case."""
    stem = Path(case.rel_path).stem
    root = flink_valid_root_path or FLINK_VALID_ROOT
    return root / case.category / stem


def flink_reference_dir(
    case: FlinkPipelineCase,
    *,
    flink_valid_root_path: Path | None = None,
) -> Path:
    return reference_pipeline_dir(case, flink_valid_root_path=flink_valid_root_path)


def iter_reference_sql_files(ref_root: Path) -> list[Path]:
    """Relative paths of ddl.*/dml.* under table dirs; skip tests/ and non-SQL."""
    if not ref_root.is_dir():
        raise FileNotFoundError(f"Flink reference pipeline not found: {ref_root}")

    found: list[Path] = []
    for path in sorted(ref_root.rglob("*.sql")):
        rel = path.relative_to(ref_root)
        if "tests" in rel.parts:
            continue
        if not _DDL_DML_RE.match(rel.name):
            continue
        found.append(rel)
    return found


def assert_structure_matches(ref_root: Path, out_dir: Path) -> None:
    """Every reference ddl/dml relative path must exist under out_dir."""
    expected = iter_reference_sql_files(ref_root)
    missing = [str(rel) for rel in expected if not (out_dir / rel).is_file()]
    assert not missing, (
        f"Missing generated SQL files under {out_dir} (expected from {ref_root}):\n"
        + "\n".join(f"  - {m}" for m in missing)
    )


def _normalize_line(line: str) -> str:
    stripped = line.strip()
    if stripped.endswith(","):
        return stripped[:-1]
    return stripped


def _is_serde_with_line(line: str) -> bool:
    lowered = line.strip().lower()
    return any(key in lowered for key in _SERDE_KEYS)


def _sql_lines_for_compare(text: str) -> set[str]:
    lines: set[str] = set()
    for raw in text.splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("--"):
            continue
        if _is_serde_with_line(stripped):
            continue
        lines.add(_normalize_line(stripped))
    return lines


def extract_primary_key(sql: str) -> tuple[str, ...] | None:
    """Parse PRIMARY KEY (...) columns (order preserved, lowercased)."""
    match = _PRIMARY_KEY_RE.search(sql)
    if not match:
        return None
    cols = []
    for part in match.group(1).split(","):
        name = part.strip().strip("`\"'").lower()
        if name:
            cols.append(name)
    return tuple(cols) if cols else None


def compare_sql_files(reference_file: Path | str, created_file: Path | str) -> dict[str, Any]:
    """Unordered line compare (serde ignored) plus strict PRIMARY KEY check."""
    ref_path = Path(reference_file)
    created_path = Path(created_file)
    ref_text = ref_path.read_text()
    created_text = created_path.read_text()

    reference_lines = _sql_lines_for_compare(ref_text)
    created_lines = _sql_lines_for_compare(created_text)
    missing = reference_lines - created_lines
    overlap = reference_lines & created_lines
    match_pct = len(overlap) / len(reference_lines) * 100 if reference_lines else 100.0

    reference_pk = extract_primary_key(ref_text)
    created_pk = extract_primary_key(created_text)
    if reference_pk is None:
        primary_key_match = True
    else:
        primary_key_match = created_pk == reference_pk

    return {
        "all_reference_lines_present": len(missing) == 0,
        "missing_lines": sorted(missing),
        "extra_lines": sorted(created_lines - reference_lines),
        "reference_count": len(reference_lines),
        "created_count": len(created_lines),
        "match_percentage": match_pct,
        "primary_key_match": primary_key_match,
        "reference_pk": reference_pk,
        "created_pk": created_pk,
    }


def assert_pipeline_matches_reference(
    case: FlinkPipelineCase,
    out_dir: Path,
    *,
    min_match: float = 80.0,
    ref_root: Path | None = None,
    flink_valid_root_path: Path | None = None,
) -> None:
    """Assert structure, PRIMARY KEY, and unordered SQL match against golden."""
    root = ref_root or reference_pipeline_dir(
        case, flink_valid_root_path=flink_valid_root_path
    )
    assert root.is_dir(), f"Flink reference pipeline not found: {root}"
    assert_structure_matches(root, out_dir)

    failures: list[str] = []
    for rel in iter_reference_sql_files(root):
        cmp = compare_sql_files(root / rel, out_dir / rel)
        if not cmp["primary_key_match"]:
            failures.append(
                f"{rel}: PRIMARY KEY mismatch "
                f"reference={cmp['reference_pk']} created={cmp['created_pk']}"
            )
        if cmp["match_percentage"] < min_match:
            failures.append(
                f"{rel}: match {cmp['match_percentage']:.1f}% < {min_match}% "
                f"missing={cmp['missing_lines']} extra={cmp['extra_lines']}"
            )

    assert not failures, (
        f"Flink golden compare failed for {case.rel_path} "
        f"(out={out_dir}, ref={root}):\n"
        + "\n".join(f"  - {f}" for f in failures)
    )


__all__ = [
    "FLINK_VALID_ROOT",
    "REPO_ROOT",
    "FlinkPipelineCase",
    "assert_pipeline_matches_reference",
    "assert_structure_matches",
    "compare_sql_files",
    "extract_primary_key",
    "find_repo_root",
    "flink_reference_dir",
    "flink_valid_root",
    "iter_reference_sql_files",
    "reference_pipeline_dir",
]
