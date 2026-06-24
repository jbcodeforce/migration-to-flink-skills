"""LLM configuration helpers."""

import pytest
from pathlib import Path
from flink_skill_common.config import HarnessContext, configure
__COMMON_ROOT = Path(__file__).resolve().parents[2]
__PROJECT_ROOT = __COMMON_ROOT.parent
configure(HarnessContext(harness_root=__COMMON_ROOT, project_root=__PROJECT_ROOT))

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
        lambda *args, **kwargs: ["Qwen3.6-27B-PARO"],
    )
    assert resolve_llm_model() == "Qwen3.6-27B-PARO"
