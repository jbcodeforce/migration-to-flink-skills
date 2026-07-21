"""
Copyright 2024-2026 Confluent, Inc.

Persist split source statements and track migration status for resume.
Shared by ksql-to-flink, spark-to-flink, and other Flink migration harnesses.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, Sequence

Status = Literal["pending", "in_progress", "migrated", "failed", "interrupted"]

MANIFEST_VERSION = 1
MANIFEST_FILENAME = "manifest.json"
_PENDING_STATUSES = frozenset({"pending", "failed", "interrupted", "in_progress"})

_logger = logging.getLogger(__name__)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class StatementEntry:
    index: int
    name: str
    file: str
    table: str
    status: Status = "pending"
    error: str | None = None
    updated_at: str = field(default_factory=_utc_now)


@dataclass
class Manifest:
    version: int
    source_file: str
    source_sha256: str
    created_at: str
    updated_at: str
    statements: list[StatementEntry]


def statements_dir_for(source: Path) -> Path:
    """Return `<parent>/<stem>.statements/` beside the source file."""
    source = source.resolve()
    return source.parent / f"{source.stem}.statements"


def source_sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def manifest_path(statements_dir: Path) -> Path:
    return statements_dir / MANIFEST_FILENAME


def _normalize_ext(statement_ext: str) -> str:
    return statement_ext if statement_ext.startswith(".") else f".{statement_ext}"


def _entry_from_dict(raw: dict) -> StatementEntry:
    return StatementEntry(
        index=int(raw["index"]),
        name=str(raw["name"]),
        file=str(raw["file"]),
        table=str(raw["table"]),
        status=raw.get("status", "pending"),  # type: ignore[arg-type]
        error=raw.get("error"),
        updated_at=str(raw.get("updated_at") or _utc_now()),
    )


def load_manifest(statements_dir: Path) -> Manifest | None:
    path = manifest_path(statements_dir)
    if not path.is_file():
        return None
    raw = json.loads(path.read_text(encoding="utf-8"))
    return Manifest(
        version=int(raw.get("version", MANIFEST_VERSION)),
        source_file=str(raw["source_file"]),
        source_sha256=str(raw["source_sha256"]),
        created_at=str(raw["created_at"]),
        updated_at=str(raw["updated_at"]),
        statements=[_entry_from_dict(s) for s in raw.get("statements", [])],
    )


def save_manifest(statements_dir: Path, manifest: Manifest) -> Path:
    statements_dir.mkdir(parents=True, exist_ok=True)
    path = manifest_path(statements_dir)
    payload = {
        "version": manifest.version,
        "source_file": manifest.source_file,
        "source_sha256": manifest.source_sha256,
        "created_at": manifest.created_at,
        "updated_at": manifest.updated_at,
        "statements": [asdict(s) for s in manifest.statements],
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _statement_filename(index: int, name: str, statement_ext: str) -> str:
    safe = "".join(c if c.isalnum() or c in "_-" else "_" for c in name)
    return f"{index:03d}_{safe}{statement_ext}"


def _build_entries(
    names: Sequence[str],
    *,
    table_override: str | None,
    statement_ext: str,
) -> list[StatementEntry]:
    total = len(names)
    entries: list[StatementEntry] = []
    for index, name in enumerate(names, start=1):
        resolved_name = name or f"statement_{index}"
        if total == 1 and table_override:
            table = table_override
        else:
            table = resolved_name
        filename = _statement_filename(index, resolved_name, statement_ext)
        entries.append(
            StatementEntry(
                index=index,
                name=resolved_name,
                file=filename,
                table=table,
                status="pending",
            )
        )
    return entries


def _write_statement_files(
    statements_dir: Path,
    statements: Sequence[str],
    entries: list[StatementEntry],
    statement_ext: str,
) -> None:
    statements_dir.mkdir(parents=True, exist_ok=True)
    for old in statements_dir.glob(f"*{statement_ext}"):
        old.unlink()
    for statement, entry in zip(statements, entries, strict=True):
        (statements_dir / entry.file).write_text(statement, encoding="utf-8")


def try_load_matching_manifest(
    source: Path,
    source_text: str,
) -> tuple[Manifest, Path] | None:
    """
    Return (manifest, statements_dir) when a manifest exists and source sha matches.

    Callers can skip re-splitting the source file and resume pending entries.
    """
    source = source.resolve()
    statements_dir = statements_dir_for(source)
    existing = load_manifest(statements_dir)
    if existing is None:
        return None
    if existing.source_sha256 != source_sha256(source_text):
        return None
    return existing, statements_dir


def init_or_load_manifest(
    source: Path,
    statements: Sequence[str],
    source_text: str,
    *,
    names: Sequence[str],
    table_override: str | None = None,
    statement_ext: str = ".sql",
) -> tuple[Manifest, Path, bool]:
    """
    Load an existing matching manifest or rebuild statement files + manifest.

    Callers supply dialect-specific ``names`` (one per statement) and
    ``statement_ext`` (e.g. ``.ksql``, ``.sql``, ``.py``).

    Returns (manifest, statements_dir, rebuilt).
    """
    if len(names) != len(statements):
        raise ValueError(
            f"names length ({len(names)}) must match statements length ({len(statements)})"
        )

    source = source.resolve()
    statements_dir = statements_dir_for(source)
    digest = source_sha256(source_text)
    existing = load_manifest(statements_dir)
    ext = _normalize_ext(statement_ext)

    if existing is not None and existing.source_sha256 == digest:
        return existing, statements_dir, False

    if existing is not None:
        _logger.warning(
            "Source file changed (sha mismatch); rebuilding statements dir at %s",
            statements_dir,
        )

    now = _utc_now()
    entries = _build_entries(names, table_override=table_override, statement_ext=ext)
    _write_statement_files(statements_dir, statements, entries, ext)
    manifest = Manifest(
        version=MANIFEST_VERSION,
        source_file=str(source),
        source_sha256=digest,
        created_at=now,
        updated_at=now,
        statements=entries,
    )
    save_manifest(statements_dir, manifest)
    return manifest, statements_dir, True


def pending_entries(manifest: Manifest) -> list[StatementEntry]:
    return [s for s in manifest.statements if s.status in _PENDING_STATUSES]


def update_status(
    statements_dir: Path,
    manifest: Manifest,
    index: int,
    status: Status,
    error: str | None = None,
) -> Manifest:
    now = _utc_now()
    for entry in manifest.statements:
        if entry.index == index:
            entry.status = status
            entry.error = error
            entry.updated_at = now
            break
    else:
        raise KeyError(f"No statement with index={index}")
    manifest.updated_at = now
    save_manifest(statements_dir, manifest)
    return manifest


def read_statement_sql(statements_dir: Path, entry: StatementEntry) -> str:
    return (statements_dir / entry.file).read_text(encoding="utf-8")
