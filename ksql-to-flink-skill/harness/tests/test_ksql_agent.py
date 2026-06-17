"""Unit tests for Agno skill loading."""

from agno.skills import LocalSkills, Skills

from ksql_flink_skill.config import skill_dir


def test_local_skills_loads_ksql_to_flink():
    skills = Skills(loaders=[LocalSkills(str(skill_dir()), validate=False)])
    names = skills.get_skill_names()
    print(names)
    assert "ksql-to-flink" in names

    skill = skills.get_skill("ksql-to-flink")
    assert skill is not None
    assert "Flink SQL" in skill.description or "Flink SQL" in skill.instructions
    assert "translation-rules.md" in skill.references
    assert "examples.md" in skill.references
