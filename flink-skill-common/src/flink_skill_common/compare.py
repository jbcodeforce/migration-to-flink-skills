"""Unordered line comparison for DDL/DML golden tests."""

from __future__ import annotations

from pathlib import Path


def _normalize_line(line: str) -> str:
    stripped = line.strip()
    if stripped.endswith(","):
        return stripped[:-1]
    return stripped


def compare_files_unordered(reference_file: Path | str, created_file: Path | str) -> dict:
    """Compare SQL files ignoring line order (same metric as shift_left tests)."""
    ref_path = Path(reference_file)
    created_path = Path(created_file)

    with ref_path.open() as f:
        reference_lines = {
            _normalize_line(line) for line in f if line.strip() and not line.strip().startswith("--")
        }
    with created_path.open() as f:
        created_lines = {
            _normalize_line(line) for line in f if line.strip() and not line.strip().startswith("--")
        }

    missing = reference_lines - created_lines
    overlap = reference_lines & created_lines
    match_pct = len(overlap) / len(reference_lines) * 100 if reference_lines else 100.0

    return {
        "all_reference_lines_present": len(missing) == 0,
        "missing_lines": sorted(missing),
        "extra_lines": sorted(created_lines - reference_lines),
        "reference_count": len(reference_lines),
        "created_count": len(created_lines),
        "match_percentage": match_pct,
    }
