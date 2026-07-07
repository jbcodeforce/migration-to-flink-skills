"""Unit tests for Agno skill adaptation and loaders."""

from pathlib import Path

from agno.skills import Skills

from flink_skill_common.agents.skill_loaders import AgnoAdaptedLocalSkills
from flink_skill_common.config import HarnessContext, configure, flink_skill_common_skill_dir
from flink_skill_common.skill_adapt import adapt_skill_content

__COMMON_ROOT = Path(__file__).resolve().parents[3]
__PROJECT_ROOT = __COMMON_ROOT.parent
configure(HarnessContext(harness_root=__COMMON_ROOT, project_root=__PROJECT_ROOT))


def test_agno_adapted_skill_strips_cursor_write_instructions():
    skill_md = (flink_skill_common_skill_dir() / "validate-flink-sql" / "SKILL.md").read_text(
        encoding="utf-8"
    )
    adapted = adapt_skill_content(skill_md, "agno")
    assert "Write corrected SQL" not in adapted
    assert "validate_flink_sql_offline" not in adapted
    assert "get_skill_script" in adapted


def test_agno_adapted_local_skills_loader():
    skills = Skills(loaders=[AgnoAdaptedLocalSkills(str(flink_skill_common_skill_dir()), validate=False)])
    validate_skill = skills.get_skill("validate-flink-sql")
    assert validate_skill is not None
    assert "Write corrected SQL" not in validate_skill.instructions
    assert "get_skill_script" in validate_skill.instructions
