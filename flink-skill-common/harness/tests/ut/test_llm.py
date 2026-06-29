"""LLM configuration helpers."""

from unittest.mock import MagicMock, patch

import pytest
from pathlib import Path
from flink_skill_common.config import HarnessContext, configure, flink_skill_common_skill_dir, skill_dir
__COMMON_ROOT = Path(__file__).resolve().parents[2]
__PROJECT_ROOT = __COMMON_ROOT.parent
configure(HarnessContext(harness_root=__COMMON_ROOT, project_root=__PROJECT_ROOT))

from flink_skill_common.agents.sources import (
    _source_ddl_prompt_template,
    build_source_ddl_agent,
    generate_source_ddls,
    source_ddl_prompt,
)
from flink_skill_common.llm import (
    LlmConfigError,
    ensure_model_context,
    is_agent_error_response,
    llm_reachable,
    resolve_llm_model,
)


def test_fetch_models_payload_sends_bearer_token(monkeypatch):
    from flink_skill_common.llm import _fetch_models_payload

    captured: dict[str, str] = {}

    class _Resp:
        def read(self):
            return b'{"data": [{"id": "test-model"}]}'

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def _urlopen(req, timeout=2.0):
        captured["authorization"] = dict(req.header_items()).get("Authorization")
        return _Resp()

    monkeypatch.setattr("flink_skill_common.llm.load_env", lambda: None)
    monkeypatch.setattr("urllib.request.urlopen", _urlopen)
    assert _fetch_models_payload("http://localhost:7999/v1", api_key="secret-key") is not None
    assert captured["authorization"] == "Bearer secret-key"


def test_llm_reachable_false_on_network_error(monkeypatch):
    import urllib.error

    def _fail(*args, **kwargs):
        raise urllib.error.URLError("No route to host")

    monkeypatch.setattr("urllib.request.urlopen", _fail)
    monkeypatch.setattr("flink_skill_common.llm.load_env", lambda: None)
    assert llm_reachable("http://10.0.0.148:7999/v1") is False


def test_llm_reachable_false_on_empty_models(monkeypatch):
    class _Resp:
        def read(self):
            return b"{}"

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr("urllib.request.urlopen", lambda *args, **kwargs: _Resp())
    monkeypatch.setattr("flink_skill_common.llm.load_env", lambda: None)
    assert llm_reachable("http://localhost:7999/v1") is False


def test_llm_reachable_true_when_models_respond(monkeypatch):
    class _Resp:
        def read(self):
            return b'{"data": [{"id": "test-model"}]}'

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr("urllib.request.urlopen", lambda *args, **kwargs: _Resp())
    monkeypatch.setattr("flink_skill_common.llm.load_env", lambda: None)
    assert llm_reachable("http://localhost:7999/v1") is True


def test_is_agent_error_response_detects_provider_failures():
    assert is_agent_error_response("Model 'foo' not found. Available models: bar")
    assert is_agent_error_response("Prompt too long: 5000 tokens exceeds max context window of 4096")
    assert not is_agent_error_response("DDL:\n```sql\nCREATE TABLE t (id STRING);\n```")


def test_resolve_llm_model_rejects_unknown_model(monkeypatch):
    monkeypatch.setenv("SL_LLM_MODEL", "definitely-not-a-model")
    monkeypatch.setattr(
        "flink_skill_common.llm.fetch_available_models",
        lambda *args, **kwargs: ["Qwen3.6-27B-4bit"],
    )
    with pytest.raises(LlmConfigError, match="Available models"):
        resolve_llm_model()


def test_ensure_model_context_rejects_small_window(monkeypatch):
    monkeypatch.setattr(
        "flink_skill_common.llm.fetch_model_context_windows",
        lambda *args, **kwargs: {"Qwen3.6-27B-4bit": 4096, "Qwen3.6-35B-A3B-UD-MLX-4bit": 262144},
    )
    with pytest.raises(LlmConfigError, match="4096 tokens"):
        ensure_model_context("Qwen3.6-27B-4bit")


def test_resolve_llm_model_normalizes_colon_name(monkeypatch):
    monkeypatch.setenv("SL_LLM_MODEL", "Qwen3.6:27b-PARO")
    monkeypatch.setattr(
        "flink_skill_common.llm.fetch_available_models",
        lambda *args, **kwargs: ["Qwen3.6-27B-PARO", "Ornith-1.0-9B-6bit"],
    )
    assert resolve_llm_model() == "Ornith-1.0-9B-6bit"


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
        ksql="CREATE STREAM s AS SELECT * FROM src;",
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
