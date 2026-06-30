"""Unit tests for adapt_skills runtime block filtering."""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[4]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.adapt_skills import adapt_skill_content, parse_skill_name


SAMPLE_SKILL = """---
name: sample-skill
description: test
---
# Sample

Shared rules stay here.

## Execution

<!-- runtime:agno -->
Use `get_skill_script('sample-skill', 'validate_offline.py', execute=True)`.
<!-- /runtime:agno -->

<!-- runtime:cursor,claude -->
Call MCP `validate_flink_sql_offline(ddls, dmls)`.
<!-- /runtime:cursor,claude -->
"""


def test_adapt_skill_content_cursor_keeps_mcp_not_agno():
    adapted = adapt_skill_content(SAMPLE_SKILL, "cursor")
    assert "validate_flink_sql_offline" in adapted
    assert "get_skill_script" not in adapted
    assert "<!-- runtime:" not in adapted


def test_adapt_skill_content_agno_keeps_script_not_mcp():
    adapted = adapt_skill_content(SAMPLE_SKILL, "agno")
    assert "get_skill_script" in adapted
    assert "validate_flink_sql_offline" not in adapted
    assert "<!-- runtime:" not in adapted


def test_parse_skill_name_from_frontmatter():
    assert parse_skill_name(SAMPLE_SKILL, "fallback") == "sample-skill"


def test_validate_flink_sql_cursor_adaptation_from_canonical():
    skill_md = (_REPO_ROOT / "flink-skill-common/skill/SKILL.md").read_text(encoding="utf-8")
    adapted = adapt_skill_content(skill_md, "cursor")
    assert "validate_flink_sql_offline" in adapted
    assert "get_skill_script" not in adapted
