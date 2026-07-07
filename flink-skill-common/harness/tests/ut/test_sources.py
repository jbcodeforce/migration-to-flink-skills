"""Unit tests for source DDL agent."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from agno.skills import Skills

from flink_skill_common.agents.skill_loaders import AgnoAdaptedLocalSkills
from flink_skill_common.config import HarnessContext, configure, flink_skill_common_skill_dir
from flink_skill_common.agents.table_source_agent import (
    _source_ddl_prompt,
    _source_ddl_skill_dir,
    generate_source_ddls,
)

__COMMON_ROOT = Path(__file__).resolve().parents[3]
__PROJECT_ROOT = __COMMON_ROOT.parent
configure(HarnessContext(harness_root=__COMMON_ROOT, project_root=__PROJECT_ROOT))


def test_source_ddl_skill_dir():
    skill_path = _source_ddl_skill_dir()
    assert skill_path.is_dir()
    assert (skill_path / "SKILL.md").is_file()
    assert "source-ddl" in str(skill_path)


def test_local_skills_loads_source_ddl():
    skills = Skills(loaders=[AgnoAdaptedLocalSkills(str(_source_ddl_skill_dir()), validate=False)])
    names = skills.get_skill_names()
    assert "source-ddl" in names
    skill = skills.get_skill("source-ddl")
    assert skill is not None
    assert "source_ddls" in skill.instructions


def test_source_ddl_prompt_includes_inputs():
    prompt = _source_ddl_prompt(
        target_table="george_martin",
        src_sql="CREATE STREAM s AS SELECT * FROM src;",
        dml_sql="INSERT INTO george_martin SELECT * FROM all_publications;",
        missing_sources=["all_publications", "authors"],
    )
    assert "target_table: george_martin" in prompt
    assert "missing_sources: [all_publications, authors]" in prompt
    assert "CREATE STREAM s AS SELECT * FROM src;" in prompt
    assert "INSERT INTO george_martin SELECT * FROM all_publications;" in prompt
    assert "sql_script:" in prompt
    assert "dml_sql:" in prompt


def test_generate_source_ddls_returns_empty_when_no_missing_sources():
    assert generate_source_ddls("t", "ksql", "dml", []) == {}


def test_generate_source_ddls_returns_parsed_ddls():
    agent = MagicMock()
    agent.run.return_value = MagicMock(
        content='{"source_ddls": [{"table": "src_st", "ddl": "CREATE TABLE IF NOT EXISTS src_st (id STRING);"}]}'
    )
    with patch("flink_skill_common.agents.table_source_agent.build_skilled_agent", return_value=agent):
        result = generate_source_ddls(
            "target",
            "ksql",
            "INSERT INTO target SELECT id FROM src_st;",
            ["src_st"],
        )
    assert result == {"src_st": "CREATE TABLE IF NOT EXISTS src_st (id STRING);"}
    agent.run.assert_called_once()


def test_generate_source_ddls_matches_table_name_case_insensitively():
    agent = MagicMock()
    agent.run.return_value = MagicMock(
        content='{"source_ddls": [{"table": "src_st", "ddl": "CREATE TABLE IF NOT EXISTS src_st (id STRING);"}]}'
    )
    with patch("flink_skill_common.agents.table_source_agent.build_skilled_agent", return_value=agent):
        result = generate_source_ddls(
            "target",
            "ksql",
            "INSERT INTO target SELECT id FROM SRC_ST;",
            ["SRC_ST"],
        )
    assert result == {"SRC_ST": "CREATE TABLE IF NOT EXISTS src_st (id STRING);"}


def test_generate_source_ddls_raises_when_llm_omits_table():
    agent = MagicMock()
    agent.run.return_value = MagicMock(content='{"source_ddls": []}')
    with patch("flink_skill_common.agents.table_source_agent.build_skilled_agent", return_value=agent):
        with pytest.raises(ValueError, match="LLM did not return DDL for source tables: missing_src"):
            generate_source_ddls("target", "ksql", "dml", ["missing_src"])


def test_build_source_ddl_agent_uses_source_ddl_skill():
    skills = Skills(loaders=[AgnoAdaptedLocalSkills(str(_source_ddl_skill_dir()), validate=False)])
    assert skills.get_skill_names() == ["source-ddl"]
    skill = skills.get_skill("source-ddl")
    assert skill is not None
    assert "source_ddls" in skill.instructions


def test_multi_skill_root_loads_both_skills():
    skills = Skills(loaders=[AgnoAdaptedLocalSkills(str(flink_skill_common_skill_dir()), validate=False)])
    names = skills.get_skill_names()
    assert "validate-flink-sql" in names
    assert "source-ddl" in names
