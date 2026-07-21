"""Unit tests for shared migration manifest init/load/resume helpers."""

from pathlib import Path

import pytest

from flink_skill_common.migration_manifest import (
    init_or_load_manifest,
    load_manifest,
    pending_entries,
    source_sha256,
    statements_dir_for,
    try_load_matching_manifest,
    update_status,
)


_TWO_STMTS = """
CREATE STREAM clicks (id INT) WITH (KAFKA_TOPIC='clicks');
CREATE STREAM detected_clicks AS SELECT * FROM clicks EMIT CHANGES;
"""

_TWO_NAMES = ["clicks", "detected_clicks"]
_TWO_PARTS = [
    "CREATE STREAM clicks (id INT) WITH (KAFKA_TOPIC='clicks');",
    "CREATE STREAM detected_clicks AS SELECT * FROM clicks EMIT CHANGES;",
]


def test_statements_dir_for(tmp_path: Path):
    source = tmp_path / "pipeline.ksql"
    assert statements_dir_for(source) == (tmp_path / "pipeline.statements").resolve()


def test_init_writes_statement_files_and_manifest(tmp_path: Path):
    source = tmp_path / "pipeline.ksql"
    source.write_text(_TWO_STMTS)

    manifest, statements_dir, rebuilt = init_or_load_manifest(
        source,
        _TWO_PARTS,
        _TWO_STMTS,
        names=_TWO_NAMES,
        statement_ext=".ksql",
    )

    assert rebuilt is True
    assert statements_dir == tmp_path / "pipeline.statements"
    assert (statements_dir / "001_clicks.ksql").is_file()
    assert (statements_dir / "002_detected_clicks.ksql").is_file()
    assert (statements_dir / "manifest.json").is_file()
    assert manifest.source_sha256 == source_sha256(_TWO_STMTS)
    assert [e.name for e in manifest.statements] == ["clicks", "detected_clicks"]
    assert [e.table for e in manifest.statements] == ["clicks", "detected_clicks"]
    assert all(e.status == "pending" for e in manifest.statements)


def test_statement_ext_sql_for_spark_style(tmp_path: Path):
    text = "CREATE TABLE t (id INT);"
    source = tmp_path / "job.sql"
    source.write_text(text)
    manifest, statements_dir, _ = init_or_load_manifest(
        source,
        [text],
        text,
        names=["t"],
        statement_ext=".sql",
    )
    assert (statements_dir / "001_t.sql").is_file()
    assert manifest.statements[0].file == "001_t.sql"


def test_single_statement_table_override(tmp_path: Path):
    text = "CREATE STREAM s (id INT) WITH (KAFKA_TOPIC='t');"
    source = tmp_path / "one.ksql"
    source.write_text(text)
    manifest, _, _ = init_or_load_manifest(
        source,
        [text],
        text,
        names=["s"],
        table_override="my_table",
        statement_ext=".ksql",
    )
    assert manifest.statements[0].table == "my_table"
    assert manifest.statements[0].name == "s"


def test_names_length_mismatch_raises(tmp_path: Path):
    source = tmp_path / "pipeline.ksql"
    source.write_text(_TWO_STMTS)
    with pytest.raises(ValueError, match="names length"):
        init_or_load_manifest(
            source,
            _TWO_PARTS,
            _TWO_STMTS,
            names=["clicks"],
            statement_ext=".ksql",
        )


def test_try_load_matching_manifest_hits_and_misses(tmp_path: Path):
    source = tmp_path / "pipeline.ksql"
    source.write_text(_TWO_STMTS)
    assert try_load_matching_manifest(source, _TWO_STMTS) is None

    init_or_load_manifest(
        source,
        _TWO_PARTS,
        _TWO_STMTS,
        names=_TWO_NAMES,
        statement_ext=".ksql",
    )
    matched = try_load_matching_manifest(source, _TWO_STMTS)
    assert matched is not None
    manifest, statements_dir = matched
    assert statements_dir == tmp_path / "pipeline.statements"
    assert len(manifest.statements) == 2

    assert try_load_matching_manifest(source, _TWO_STMTS + "\n-- changed\n") is None


def test_load_matching_manifest_does_not_rebuild(tmp_path: Path):
    source = tmp_path / "pipeline.ksql"
    source.write_text(_TWO_STMTS)
    first, statements_dir, _ = init_or_load_manifest(
        source,
        _TWO_PARTS,
        _TWO_STMTS,
        names=_TWO_NAMES,
        statement_ext=".ksql",
    )
    update_status(statements_dir, first, 1, "migrated")

    second, _, rebuilt = init_or_load_manifest(
        source,
        _TWO_PARTS,
        _TWO_STMTS,
        names=_TWO_NAMES,
        statement_ext=".ksql",
    )
    assert rebuilt is False
    assert second.statements[0].status == "migrated"
    assert second.statements[1].status == "pending"
    assert len(pending_entries(second)) == 1
    assert pending_entries(second)[0].name == "detected_clicks"


def test_sha_mismatch_rebuilds_and_resets(tmp_path: Path):
    source = tmp_path / "pipeline.ksql"
    source.write_text(_TWO_STMTS)
    first, statements_dir, _ = init_or_load_manifest(
        source,
        _TWO_PARTS,
        _TWO_STMTS,
        names=_TWO_NAMES,
        statement_ext=".ksql",
    )
    update_status(statements_dir, first, 1, "migrated")

    new_text = _TWO_STMTS + "\n-- changed\n"
    source.write_text(new_text)
    second, _, rebuilt = init_or_load_manifest(
        source,
        _TWO_PARTS,
        new_text,
        names=_TWO_NAMES,
        statement_ext=".ksql",
    )
    assert rebuilt is True
    assert all(e.status == "pending" for e in second.statements)
    assert second.source_sha256 == source_sha256(new_text)
    reloaded = load_manifest(statements_dir)
    assert reloaded is not None
    assert reloaded.source_sha256 == second.source_sha256


def test_update_status_persists(tmp_path: Path):
    source = tmp_path / "one.ksql"
    text = "CREATE STREAM s (id INT) WITH (KAFKA_TOPIC='t');"
    source.write_text(text)
    manifest, statements_dir, _ = init_or_load_manifest(
        source,
        [text],
        text,
        names=["s"],
        statement_ext=".ksql",
    )
    update_status(statements_dir, manifest, 1, "failed", error="boom")
    reloaded = load_manifest(statements_dir)
    assert reloaded is not None
    assert reloaded.statements[0].status == "failed"
    assert reloaded.statements[0].error == "boom"
