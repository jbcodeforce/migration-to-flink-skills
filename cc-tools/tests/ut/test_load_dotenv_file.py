"""Unit tests for cc_deploy dotenv resolution."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from cc_deploy.deploy_flink_statements import find_repo_root, load_dotenv_file, resolve_dotenv_path


def test_find_repo_root_from_nested(tmp_path):
    marker = tmp_path / "references" / "flink" / "valid"
    marker.mkdir(parents=True)
    nested = tmp_path / "cc-tools" / "src"
    nested.mkdir(parents=True)
    assert find_repo_root(nested) == tmp_path.resolve()


def test_resolve_dotenv_defaults_to_repo_root_env(tmp_path, monkeypatch):
    monkeypatch.delenv("DOTENV_FILE", raising=False)
    monkeypatch.delenv("CONFLUENT_ENV_FILE", raising=False)
    marker = tmp_path / "references" / "flink" / "valid"
    marker.mkdir(parents=True)
    env_file = tmp_path / ".env"
    env_file.write_text("CC_DOTENV_TEST=from-repo\n")
    assert resolve_dotenv_path(tmp_path) == env_file


def test_resolve_dotenv_respects_dotenv_file(tmp_path, monkeypatch):
    marker = tmp_path / "references" / "flink" / "valid"
    marker.mkdir(parents=True)
    (tmp_path / ".env").write_text("CC_DOTENV_TEST=default\n")
    external = tmp_path / "override.env"
    external.write_text("CC_DOTENV_TEST=override\n")
    monkeypatch.setenv("DOTENV_FILE", str(external))
    assert resolve_dotenv_path(tmp_path) == external


def test_load_dotenv_file_loads_repo_env(tmp_path, monkeypatch):
    monkeypatch.delenv("DOTENV_FILE", raising=False)
    monkeypatch.delenv("CONFLUENT_ENV_FILE", raising=False)
    monkeypatch.delenv("CC_DOTENV_TEST", raising=False)
    marker = tmp_path / "references" / "flink" / "valid"
    marker.mkdir(parents=True)
    (tmp_path / ".env").write_text("CC_DOTENV_TEST=from-repo\n")
    # Point finder at tmp_path by starting under it
    monkeypatch.chdir(tmp_path)
    assert load_dotenv_file(start=tmp_path / "cc-tools") is True
    assert os.getenv("CC_DOTENV_TEST") == "from-repo"


def test_load_dotenv_ignores_confluent_env_file(tmp_path, monkeypatch):
    monkeypatch.delenv("DOTENV_FILE", raising=False)
    marker = tmp_path / "references" / "flink" / "valid"
    marker.mkdir(parents=True)
    (tmp_path / ".env").write_text("CC_DOTENV_TEST=from-repo\n")
    confluent = tmp_path / "confluent.env"
    confluent.write_text("CC_DOTENV_TEST=from-confluent\n")
    monkeypatch.setenv("CONFLUENT_ENV_FILE", str(confluent))
    monkeypatch.delenv("CC_DOTENV_TEST", raising=False)
    assert load_dotenv_file(start=tmp_path) is True
    assert os.getenv("CC_DOTENV_TEST") == "from-repo"
