from pathlib import Path
from unittest.mock import patch
from agno.skills import Skills

from flink_skill_common.agents.skill_loaders import AgnoAdaptedLocalSkills
from flink_skill_common.agents.cc_deployment_fixer import build_deploy_fixer_agent
from flink_skill_common.config import (
    HarnessContext,
    configure,
    flink_skill_common_skill_dir,
    validate_flink_sql_skill_dir,
)

_COMMON_ROOT = Path(__file__).resolve().parents[2]
_PROJECT_ROOT = _COMMON_ROOT.parent
configure(HarnessContext(harness_root=_COMMON_ROOT, project_root=_PROJECT_ROOT))


def test_flink_skill_common_skill_dir():
    skill_path = flink_skill_common_skill_dir()
    assert skill_path.is_dir()
    assert (validate_flink_sql_skill_dir() / "SKILL.md").is_file()
    assert "flink-skill-common/skill" in str(skill_path)


def test_local_skills_loads_validate_flink_sql():
    skills = Skills(loaders=[AgnoAdaptedLocalSkills(str(flink_skill_common_skill_dir()), validate=False)])
    names = skills.get_skill_names()
    assert "validate-flink-sql" in names
    assert "source-ddl" in names

    skill = skills.get_skill("validate-flink-sql")
    assert skill is not None
    assert "Flink" in skill.description or "Flink" in skill.instructions
    assert "confluent-sql-deploy.md" in skill.references
    assert "validate_offline.py" in skill.scripts
    assert "validate_remote.py" in skill.scripts
    assert "Write corrected SQL" not in skill.instructions


def test_build_deploy_fixer_agent():
    with patch("flink_skill_common.agents.cc_deployment_fixer.resolve_llm_model", return_value="Ornith-1.0-9B-6bit"):
        agent = build_deploy_fixer_agent()
        assert agent is not None
        assert agent.name == "FlinkSqlDeployFixerAgent"
        assert agent.model is not None
        assert agent.skills is not None
        assert len(agent.tools) >= 7
        assert agent.instructions is not None
        assert agent.markdown is True
        assert any("write_file" in instr.lower() for instr in agent.instructions)
