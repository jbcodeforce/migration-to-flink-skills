"""Offline tests for Flink statement naming and deploy order."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from flink_skill_common.deploy.flink_statement_manager import FlinkStatementManager
from flink_skill_common.deploy.statements import (
    ddl_statement_name,
    dml_statement_name,
    discover_source_ddl_files,
    normalize_statement_prefix,
)


def test_normalize_statement_prefix_replaces_underscores():
    assert normalize_statement_prefix("kes_ice_chat_deal") == "kes-ice-chat-deal"


def test_statement_names():
    assert ddl_statement_name("kes_ice_chat_deal") == "kes-ice-chat-deal-ddl"
    assert dml_statement_name("dim_all_songs") == "dim-all-songs-dml"


def test_discover_source_ddl_files(tmp_path: Path):
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    (tests_dir / "ddl.kes_ice_chat_deal_st.sql").write_text("CREATE TABLE t;")
    (tests_dir / "ddl.other_src.sql").write_text("CREATE TABLE o;")
    discovered = discover_source_ddl_files(tests_dir)
    assert discovered == [
        ("kes_ice_chat_deal_st", tests_dir / "ddl.kes_ice_chat_deal_st.sql"),
        ("other_src", tests_dir / "ddl.other_src.sql"),
    ]


def test_deploy_order_sources_before_target(tmp_path: Path):
    out = tmp_path / "out"
    tests_dir = out / "tests"
    tests_dir.mkdir(parents=True)
    (tests_dir / "ddl.src_st.sql").write_text(
        "CREATE TABLE IF NOT EXISTS src_st (id STRING);"
    )
    ddl_path = out / "ddl.target.sql"
    dml_path = out / "dml.target.sql"
    ddl_path.write_text("CREATE TABLE IF NOT EXISTS target (id STRING);")
    dml_path.write_text("INSERT INTO target SELECT id FROM src_st;")

    manager = MagicMock(spec=FlinkStatementManager)
    manager._deploy_source_ddls = lambda tests_dir, messages: FlinkStatementManager._deploy_source_ddls(
        manager, tests_dir, messages
    )
    manager._wait_for_deploy_phase = lambda statement_name: FlinkStatementManager._wait_for_deploy_phase(
        manager, statement_name
    )
    manager.deploy_table = lambda *args, **kwargs: FlinkStatementManager.deploy_table(
        manager, *args, **kwargs
    )
    create_calls: list[str] = []

    def track_create(name: str, sql: str):
        create_calls.append(name)
        return {"name": name, "phase": "COMPLETED", "detail": ""}

    manager.create_statement.side_effect = track_create
    manager.wait_for_phase.side_effect = lambda name, phases: {
        "name": name,
        "phase": "COMPLETED",
        "detail": "",
    }
    manager.check_statement_health.return_value = {
        "statement_name": dml_statement_name("target"),
        "phase": "RUNNING",
        "healthy": True,
        "detail": "",
    }

    result = manager.deploy_table("target", ddl_path, dml_path, tests_dir=tests_dir)

    assert create_calls[0] == ddl_statement_name("src_st")
    assert create_calls[1] == ddl_statement_name("target")
    assert create_calls[2] == dml_statement_name("target")
    assert len(result.source_statements) == 1
    assert result.source_statements[0][0] == ddl_statement_name("src_st")
