"""Golden fixture pair registry helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class GoldenPair:
    name: str
    source_file: Path
    flink_ddl: Path
    flink_dml: Path
    table_name: str


def assert_fixtures_exist(pairs: Iterable[GoldenPair]) -> None:
    """Raise FileNotFoundError if any golden fixture path is missing."""
    missing = []
    for pair in pairs:
        for path in (pair.source_file, pair.flink_ddl, pair.flink_dml):
            if not path.exists():
                missing.append(str(path))
    if missing:
        raise FileNotFoundError("Missing fixture files:\n" + "\n".join(missing))
