"""Unit tests for Agno skill loading."""

from flink_skill_common.agents.skill_loaders import AgnoAdaptedLocalSkills
from flink_skill_common.config import flink_skill_common_skill_dir, get_context, skill_dir, skill_md_path
from agno.skills import Skills

from spark_to_flink.migrate_agent import build_spark_migrate_agent


def test_config():
    ctx = get_context()
    assert ctx.harness_root.name == "spark-to-flink-skill"
    assert ctx.project_root.name == "migration-to-flink-skills"
    assert skill_md_path().is_file()


def test_local_skills_loads_spark_to_flink():
    skills = Skills(loaders=[AgnoAdaptedLocalSkills(str(skill_dir()), validate=False)])
    names = skills.get_skill_names()
    assert "spark-to-flink" in names

    skill = skills.get_skill("spark-to-flink")
    assert skill is not None
    assert "Flink SQL" in skill.description or "Flink SQL" in skill.instructions
    assert "translation-rules.md" in skill.references
    assert "validation-rules.md" in skill.references
    assert "examples.md" in skill.references


def test_production_agent_loads_translation_skill_only():
    assert skill_md_path().is_file()
    agent = build_spark_migrate_agent()
    assert agent.name == "SparkToFlinkAgent"
    assert agent.skills is not None
    names = agent.skills.get_skill_names()
    assert names == ["spark-to-flink"]
    skill = agent.skills.get_skill("spark-to-flink")
    assert skill is not None
    assert skill.instructions
    assert agent.tools == [] or list(agent.tools) == []


def test_common_skills_available_from_flink_skill_common():
    skills = Skills(
        loaders=[AgnoAdaptedLocalSkills(str(flink_skill_common_skill_dir()), validate=False)]
    )
    names = skills.get_skill_names()
    assert "validate-flink-sql" in names
    assert "source-ddl" in names
