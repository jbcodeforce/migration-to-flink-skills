"""LLM configuration helpers."""

from unittest.mock import MagicMock, patch

import pytest
from pathlib import Path
from flink_skill_common.config import HarnessContext, configure, flink_skill_common_skill_dir, skill_dir, llm_reachable,
__COMMON_ROOT = Path(__file__).resolve().parents[3]
__PROJECT_ROOT = __COMMON_ROOT.parent
configure(HarnessContext(harness_root=__COMMON_ROOT, project_root=__PROJECT_ROOT))

from flink_skill_common.agents.sources import (
    _source_ddl_prompt_template,
    build_source_ddl_agent,
    generate_source_ddls,
    source_ddl_prompt,
)


def test_source_ddl_prompt_template_uses_common_skill_dir():
    ksql_project = __PROJECT_ROOT / "ksql-to-flink-skill"
    ksql_root = ksql_project / "harness"
    configure(HarnessContext(harness_root=ksql_root, project_root=ksql_project))
    try:
        assert "ksql-to-flink-skill/skill" in str(skill_dir())
        expected = (flink_skill_common_skill_dir() / "prompts/source_ddl.txt").read_text()
        assert _source_ddl_prompt_template() == expected
        assert "flink-skill-common/skill" in str(flink_skill_common_skill_dir())
    finally:
        configure(HarnessContext(harness_root=__COMMON_ROOT, project_root=__PROJECT_ROOT))


def test_source_ddl_prompt_includes_inputs():
    prompt = source_ddl_prompt(
        target_table="george_martin",
        src_sql="CREATE STREAM s AS SELECT * FROM src;",
        dml_sql="INSERT INTO george_martin SELECT * FROM all_publications;",
        missing_sources=["all_publications", "authors"],
    )
    assert "target_table: george_martin" in prompt
    assert "missing_sources: [all_publications, authors]" in prompt
    assert "CREATE STREAM s AS SELECT * FROM src;" in prompt
    assert "INSERT INTO george_martin SELECT * FROM all_publications;" in prompt
    assert _source_ddl_prompt_template().splitlines()[0] in prompt


def test_generate_source_ddls_returns_empty_when_no_missing_sources():
    assert generate_source_ddls("t", "ksql", "dml", []) == {}


def test_generate_source_ddls_returns_parsed_ddls():
    agent = MagicMock()
    agent.run.return_value = MagicMock(
        content='{"source_ddls": [{"table": "src_st", "ddl": "CREATE TABLE IF NOT EXISTS src_st (id STRING);"}]}'
    )
    with patch("flink_skill_common.agents.sources.build_source_ddl_agent", return_value=agent):
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
    with patch("flink_skill_common.agents.sources.build_source_ddl_agent", return_value=agent):
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
    with patch("flink_skill_common.agents.sources.build_source_ddl_agent", return_value=agent):
        with pytest.raises(ValueError, match="LLM did not return DDL for source tables: missing_src"):
            generate_source_ddls("target", "ksql", "dml", ["missing_src"])


def test_build_source_ddl_agent():
    fake_agent = MagicMock(name="SourceDdlAgent")
    fake_model = MagicMock()
    with patch("flink_skill_common.agents.sources._make_model", return_value=fake_model):
        with patch("flink_skill_common.agents.sources.Agent", return_value=fake_agent) as mock_agent:
            agent = build_source_ddl_agent()
    assert agent is fake_agent
    mock_agent.assert_called_once_with(
        name="SourceDdlAgent",
        model=fake_model,
        instructions=[
            "Generate Flink CREATE TABLE IF NOT EXISTS DDL stubs for upstream source tables.",
            "Follow the JSON output format in the user prompt exactly.",
            "Respond with JSON only — no markdown fences or explanations.",
        ],
    )
