"""
Integration tests for snapshot and streaming query CLIs against raw_classical_songs.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from cc_deploy.deploy_flink_statements import load_dotenv_file
from cc_deploy.flink_deploy import flink_connection, get_config, read_sql, run_create
from cc_deploy.run_snapshot_query import main as snapshot_main
from cc_deploy.run_streaming_query import main as streaming_main

REPO_ROOT = Path(__file__).resolve().parents[3]
SEED = REPO_ROOT / "references" / "flink" / "valid" / "seeds" / "raw_classical_songs"
TABLE = "raw_classical_songs"


@pytest.fixture(scope="module")
def seeded_classical_songs() -> None:
    load_dotenv_file()
    config = get_config()
    with flink_connection(config) as conn:
        run_create(
            conn,
            config,
            "it-seed-raw-classical-songs-ddl",
            read_sql(SEED, "ddl.raw_classical_songs.sql"),
        )
        run_create(
            conn,
            config,
            "it-seed-raw-classical-songs-dml",
            read_sql(SEED, "dml.raw_classical_songs.sql"),
        )


def test_run_snapshot_query_cli(seeded_classical_songs, monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_snapshot_query",
            "--table",
            TABLE,
            "--columns",
            "artist, title",
            "--limit",
            "5",
            "--output",
            "json",
            "--quiet-meta",
            "--statement-name",
            "it-snapshot-cli-raw-classical-songs",
        ],
    )
    snapshot_main()

    rows = json.loads(capsys.readouterr().out)
    assert isinstance(rows, list)
    assert len(rows) >= 1
    assert "artist" in rows[0]
    assert "title" in rows[0]


def test_run_streaming_query_cli(seeded_classical_songs, monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_streaming_query",
            "--table",
            TABLE,
            "--columns",
            "artist, title",
            "--max-rows",
            "2",
            "--output",
            "json",
            "--quiet-meta",
            "--statement-name",
            "it-stream-cli-raw-classical-songs",
        ],
    )
    streaming_main()

    lines = [line for line in capsys.readouterr().out.strip().splitlines() if line.strip()]
    assert len(lines) == 2
    for line in lines:
        row = json.loads(line)
        assert "artist" in row
        assert "title" in row
