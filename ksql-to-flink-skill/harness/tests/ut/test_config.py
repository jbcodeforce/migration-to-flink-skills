

from pathlib import Path
from flink_skill_common.config import (
    skill_dir, 
    skill_md_path, 
    agent_fixer_enabled, 
    agent_fixer_max_retries,
     HarnessContext,
     configure
)

from agno.skills import LocalSkills, Skills


_HARNESS_ROOT = Path(__file__).resolve().parents[2]
_PROJECT_ROOT = _HARNESS_ROOT.parent

configure(HarnessContext(harness_root=_HARNESS_ROOT, project_root=_PROJECT_ROOT))
from flink_skill_common.config import cli_log_file, cli_log_level
from ksql_to_flink.migrate_agent import build_ksql_migrate_agent, build_deploy_retry_agent


def test_config():

    assert "ksql-to-flink-skill/skill" in str(skill_dir())
    assert "ksql-to-flink-skill/skill/SKILL.md" in str(skill_md_path())
    assert agent_fixer_enabled() == False
    assert agent_fixer_max_retries() == 2
    assert "harness/logs/ksql-flink-cli.log" in str(cli_log_file())
    assert cli_log_level() == "DEBUG"


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
    print(skill)

def test_build_migration_agent():
    agent = build_ksql_migrate_agent()
    assert agent is not None
    assert agent.name == "KsqlToFlinkAgent"
    assert agent.model is not None
    assert agent.skills is not None
    assert len(agent.tools) == 0
    assert agent.instructions is not None
    assert agent.markdown is True

def test_build_deploy_retry_agent():
    agent = build_deploy_retry_agent()
    assert agent is not None
    assert agent.name == "KsqlToFlinkDeployAgent"
    assert agent.model is not None
    assert agent.skills is not None
    assert len(agent.tools) >= 7
    assert agent.instructions is not None
    assert agent.markdown is True
    print(agent.instructions)