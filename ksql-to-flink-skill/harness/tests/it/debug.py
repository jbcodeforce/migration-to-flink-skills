from pathlib import Path
import os

from typer.testing import CliRunner

_HARNESS_ROOT = Path(__file__).resolve().parents[2]
_PROJECT_ROOT = _HARNESS_ROOT.parent

from flink_skill_common.config import (
    HarnessContext,
    configure,
    configure_cli_logging,
    llm_api_key,
    llm_base_url,
)

configure(HarnessContext(harness_root=_HARNESS_ROOT, project_root=_PROJECT_ROOT))
configure_cli_logging("ksql_to_flink.cli")

from flink_skill_common.llm import llm_reachable
from ksql_to_flink.cli import app

base_url = llm_base_url()
api_key = llm_api_key()
print(f"SL_LLM_BASE_URL={base_url}")
print(f"SL_LLM_API_KEY={'*' * max(len(api_key) - 4, 0)}{api_key[-4:] if api_key else '(empty)'}")
if not llm_reachable():
    raise SystemExit(
        f"LLM not reachable at {base_url}/models — "
        "check VPN, host, port, and that the server is running.\n"
        "If curl works in Terminal but not in Cursor debug, use launch config "
        "'k2f debug.py (start + attach)' (or run tests/it/run_debug_attach.sh "
        "then 'k2f debug.py (attach only)')."
    )

ksql_file = (
    os.getenv("HOME")
    + "/Documents/Code/MyAIAssistant/workspaces/biz-db/docs/notes/"
    "kaes-koch-ag-energy-solutions/notes/ksql/terminal_throughput_target_table/"
    "terminal_throughput_history.sql"
)
print(ksql_file)
out_dir = Path("output/debug")

runner = CliRunner()

result = runner.invoke(
    app,
    [
        "--table",
        "trip_ref",
        "--file",
        ksql_file,
        "--out-dir",
        str(out_dir),
        "--skip-deploy",
    ],
)

print(result.output)
