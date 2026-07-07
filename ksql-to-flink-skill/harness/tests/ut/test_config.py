
from flink_skill_common.agents.skill_loaders import AgnoAdaptedLocalSkills
from flink_skill_common.config import (
    agent_fixer_enabled,
    agent_fixer_max_retries,
    cli_log_file,
    cli_log_level,
    get_context,
    skill_dir,
    skill_md_path,
)
from agno.skills import Skills

from ksql_to_flink.migrate_agent import build_ksql_migrate_agent


def test_config():
    ctx = get_context()
    assert ctx.harness_root.name == "ksql-to-flink-skill"
    assert ctx.project_root.name == "migration-to-flink-skills"
    assert "ksql-to-flink-skill/skill" in str(skill_dir())
    assert "ksql-to-flink-skill/skill/SKILL.md" in str(skill_md_path())
    assert agent_fixer_enabled() == True
    assert agent_fixer_max_retries() == 2
    assert "ksql-to-flink-skill/logs/ksql-flink-cli.log" in str(cli_log_file()).replace("\\", "/")
    assert cli_log_level() == "DEBUG"


def test_local_skills_loads_ksql_to_flink():
    skills = Skills(loaders=[AgnoAdaptedLocalSkills(str(skill_dir()), validate=False)])
    names = skills.get_skill_names()
    assert "ksql-to-flink" in names

    skill = skills.get_skill("ksql-to-flink")
    assert skill is not None
    assert "Flink SQL" in skill.description or "Flink SQL" in skill.instructions


def test_production_agent_loads_translation_skill_only():
    assert skill_md_path().is_file()
    agent = build_ksql_migrate_agent()
    assert agent.skills is not None
    names = agent.skills.get_skill_names()
    assert names == ["ksql-to-flink"]
    skill = agent.skills.get_skill("ksql-to-flink")
    assert skill is not None
    assert skill.instructions


def test_build_migration_agent():
    agent = build_ksql_migrate_agent()
    assert agent is not None
    assert agent.name == "KsqlToFlinkAgent"
    assert agent.model is not None
    assert agent.skills is not None
    assert len(agent.tools) == 0
    assert agent.instructions is not None
    assert agent.markdown is True
