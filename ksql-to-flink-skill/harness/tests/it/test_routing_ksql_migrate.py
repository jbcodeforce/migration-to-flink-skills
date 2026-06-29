"""Offline CLI tests for migrate command deploy wiring."""

from pathlib import Path

from flink_skill_common.config import HarnessContext, configure, get_context
from flink_skill_common.output import extract_sql_blocks

from live_cli_runner import LiveCliRunner

_HARNESS_ROOT = Path(__file__).resolve().parents[2]
_PROJECT_ROOT = _HARNESS_ROOT.parent.parent

configure(HarnessContext(harness_root=_HARNESS_ROOT, project_root=_PROJECT_ROOT))

from ksql_to_flink.cli import app
from ksql_to_flink.ksql_utils import clean_ksql_input

runner = LiveCliRunner()

import os

# Remove the CLI log file on a new execution to ensure clean logs for this test session.
log_path = _HARNESS_ROOT / "logs" / "ksql-flink-cli.log"
try:
    if log_path.exists():
        os.remove(log_path)
except Exception as e:
    print(f"Warning: could not remove log file {log_path}: {e}")

def _test_llm_reachable():
    from flink_skill_common.llm import llm_reachable
    assert llm_reachable() == True

def _test_run_migration():
    from ksql_to_flink.migrate_agent import run_migration
    ksql_file = _PROJECT_ROOT / "references" / "ksql" / "sources" / "routing" / "filtering.ksql"
    cleaned = clean_ksql_input(ksql_file.read_text())
    resp = run_migration("george_martin", cleaned)
    print(resp)
    
    assert resp is not None
    ddls, dmls = extract_sql_blocks(resp)
    assert ddls or dmls
    for ddl in ddls:
        print(f"DDL: {ddl}")
        assert "CREATE TABLE" in ddl
    for dml in dmls:
        print(f"DML: {dml}")
        assert "INSERT INTO" in dml

def test_migrate_filtering():
    ksql_file = _PROJECT_ROOT / "references" / "ksql" / "sources" / "routing" / "filtering.ksql"
    out_dir = _PROJECT_ROOT  / "staging" / "ksqk" / "routing" / "filtering"
    print(get_context())
    result = runner.invoke(
        app,
        [
            "--table",
            "george_martin",
            "--file",
            str(ksql_file),
            "--out-dir",
            str(out_dir),
        ],
    )

    assert result.exit_code == 0