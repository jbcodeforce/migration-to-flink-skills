#!/usr/bin/env python3
"""Verify migration CLI harness dependencies, Agno agents, and LLM reachability."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def _step(label: str) -> None:
    print(f"==> {label}")


def _fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    sys.exit(1)


_SUBPROCESS_TIMEOUT = 60


def _run_cli(harness_dir: Path, cli: str) -> None:
    env = {k: v for k, v in __import__("os").environ.items() if k != "VIRTUAL_ENV"}
    result = subprocess.run(
        ["uv", "run", cli, "--help"],
        cwd=harness_dir,
        capture_output=True,
        text=True,
        env=env,
        timeout=_SUBPROCESS_TIMEOUT,
    )
    if result.returncode != 0:
        stderr = (result.stderr or result.stdout or "").strip()
        _fail(f"{cli} --help failed in {harness_dir}: {stderr}")


def _run_python(harness_dir: Path, code: str, *, label: str) -> None:
    env = {k: v for k, v in __import__("os").environ.items() if k != "VIRTUAL_ENV"}
    result = subprocess.run(
        ["uv", "run", "python", "-c", code],
        cwd=harness_dir,
        capture_output=True,
        text=True,
        env=env,
        timeout=_SUBPROCESS_TIMEOUT,
    )
    if result.returncode != 0:
        stderr = (result.stderr or result.stdout or "").strip()
        _fail(f"{label} failed in {harness_dir}: {stderr}")


def verify_imports() -> None:
    _step("Checking Python package imports")
    import agno  # noqa: F401
    import confluent_sql  # noqa: F401
    import flink_skill_common  # noqa: F401
    import sqlglot  # noqa: F401

    print(f"  agno {agno.__version__ if hasattr(agno, '__version__') else 'ok'}")
    print("  flink_skill_common, sqlglot, confluent_sql ok")


def verify_agno_agents(repo_root: Path) -> None:
    _step("Building Agno migration agents (no LLM call)")

    ksql_harness = repo_root / "ksql-to-flink-skill" / "harness"
    spark_harness = repo_root / "spark-to-flink-skill" / "harness"

    agent_code = """
from pathlib import Path
from flink_skill_common.agents.factory import build_migration_agent, make_openai_model
from flink_skill_common.config import HarnessContext, configure, llm_api_key, llm_base_url

harness_root = Path({harness!r})
project_root = harness_root.parent
configure(HarnessContext(harness_root=harness_root, project_root=project_root))
model = make_openai_model(
    base_url=llm_base_url(),
    api_key=llm_api_key(),
    model_id="setup-verify",
)
agent = build_migration_agent(
    name={name!r},
    skill_dir=project_root / "skill",
    instructions=["Setup verification only."],
    model=model,
)
print({ok_msg!r})
"""

    _run_python(
        ksql_harness,
        agent_code.format(
            harness=str(ksql_harness),
            name="KsqlToFlinkAgent",
            ok_msg="ksql-to-flink agent ok",
        ),
        label="ksql Agno agent",
    )

    _run_python(
        spark_harness,
        agent_code.format(
            harness=str(spark_harness),
            name="SparkToFlinkAgent",
            ok_msg="spark-to-flink agent ok",
        ),
        label="spark Agno agent",
    )


def _verify_entry_points(harness_dir: Path, dist_name: str, cli_names: list[str], *, label: str) -> None:
    names_repr = repr(cli_names)
    code = f"""
from importlib.metadata import entry_points
installed = {{ep.name for ep in entry_points(group="console_scripts") if ep.dist.name == {dist_name!r}}}
expected = set({names_repr})
missing = expected - installed
if missing:
    raise SystemExit(f"Missing console scripts: {{sorted(missing)}}")
print("ok: " + ", ".join(sorted(expected)))
"""
    _run_python(harness_dir, code, label=label)


def verify_clis(repo_root: Path) -> None:
    _step("Checking migration CLI entry points")

    common_harness = repo_root / "flink-skill-common" / "harness"
    ksql_harness = repo_root / "ksql-to-flink-skill" / "harness"
    spark_harness = repo_root / "spark-to-flink-skill" / "harness"

    mcp_code = """
import flink_skill_common.mcp.server  # noqa: F401
print("flink-skill-mcp ok")
"""
    _run_python(common_harness, mcp_code, label="flink-skill-mcp")
    _verify_entry_points(
        common_harness,
        "flink-skill-common",
        ["flink-skill-mcp"],
        label="flink-skill-mcp entry point",
    )

    _run_cli(ksql_harness, "ksql-flink-migrate")
    print("  ksql-flink-migrate ok")
    _verify_entry_points(
        ksql_harness,
        "ksql-flink-skill",
        ["ksql-flink-migrate", "ksql-flink-agent"],
        label="ksql CLI entry points",
    )
    print("  ksql-flink-agent ok")

    _run_cli(spark_harness, "spark-flink-migrate")
    print("  spark-flink-migrate ok")
    _verify_entry_points(
        spark_harness,
        "spark-flink-skill",
        ["spark-flink-migrate", "spark-flink-agent"],
        label="spark CLI entry points",
    )
    print("  spark-flink-agent ok")


def verify_llm(repo_root: Path) -> None:
    _step("Checking LLM reachability (strict)")

    common_harness = repo_root / "flink-skill-common" / "harness"
    llm_code = """
from pathlib import Path
from flink_skill_common.config import HarnessContext, configure, llm_base_url
from flink_skill_common.llm import (
    LlmConfigError,
    ensure_model_context,
    llm_reachable,
    resolve_llm_model,
)

repo_root = Path(%r)
common_harness = repo_root / "flink-skill-common" / "harness"
configure(
    HarnessContext(
        harness_root=common_harness,
        project_root=common_harness.parent,
    )
)
base = llm_base_url()
if not llm_reachable():
    raise SystemExit(
        f"LLM not reachable at {base}/models. "
        "Start oMLX or your OpenAI-compatible server and check SL_LLM_BASE_URL."
    )
try:
    model = resolve_llm_model()
except LlmConfigError as exc:
    raise SystemExit(str(exc)) from exc
try:
    ensure_model_context(model, min_tokens=8000)
except LlmConfigError as exc:
    raise SystemExit(str(exc)) from exc
print(f"  LLM ok at {base} model={model}")
""" % str(
        repo_root
    )

    _run_python(common_harness, llm_code, label="LLM check")


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify migration CLI setup")
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Repository root (default: parent of scripts/)",
    )
    args = parser.parse_args()
    repo_root = args.repo_root.resolve()

    if not (repo_root / "flink-skill-common").is_dir():
        _fail(f"Invalid repo root (missing flink-skill-common): {repo_root}")

    verify_imports()
    verify_agno_agents(repo_root)
    verify_clis(repo_root)
    verify_llm(repo_root)
    print("\nAll setup checks passed.")


if __name__ == "__main__":
    main()
