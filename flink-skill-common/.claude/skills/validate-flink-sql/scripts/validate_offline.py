#!/usr/bin/env python3
"""Offline Flink SQL validation via flink-skill-validate CLI."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_HARNESS_DIR = _SCRIPT_DIR.parent.parent / "harness"
_REPO_ROOT = _SCRIPT_DIR.parent.parent.parent


def main() -> None:
    cmd = [
        "uv",
        "run",
        "--directory",
        str(_HARNESS_DIR),
        "flink-skill-validate",
        "offline",
        *sys.argv[1:],
    ]
    result = subprocess.run(cmd, cwd=_REPO_ROOT, check=False)
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
