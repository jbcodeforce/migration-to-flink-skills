"""LLM configuration helpers."""

import pytest

from flink_skill_common.llm import (
    LlmConfigError,
    ensure_model_context,
    is_agent_error_response,
    resolve_llm_model,
)


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
    monkeypatch.setenv("SL_LLM_MODEL", "Qwen3.6:27b-4bit")
    monkeypatch.setattr(
        "flink_skill_common.llm.fetch_available_models",
        lambda *args, **kwargs: ["Qwen3.6-27B-4bit"],
    )
    assert resolve_llm_model() == "Qwen3.6-27B-4bit"
