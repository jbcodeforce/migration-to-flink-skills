"""OpenAI-compatible LLM configuration helpers."""

from __future__ import annotations

import json
from typing import Optional

from flink_skill_common.config import llm_base_url, llm_model, load_env


class LlmConfigError(RuntimeError):
    """Raised when SL_LLM_MODEL is missing or invalid for the configured server."""


def _fetch_models_payload(base_url: Optional[str] = None, timeout: float = 2.0) -> dict:
    import urllib.error
    import urllib.request

    url = (base_url or llm_base_url()).rstrip("/") + "/models"
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_available_models(base_url: Optional[str] = None, timeout: float = 2.0) -> list[str]:
    """Return model ids from an OpenAI-compatible /models endpoint."""
    return list(fetch_model_context_windows(base_url, timeout=timeout).keys())


def fetch_model_context_windows(
    base_url: Optional[str] = None, timeout: float = 2.0
) -> dict[str, int]:
    """Return model id -> max context window from /models metadata."""
    try:
        payload = _fetch_models_payload(base_url, timeout=timeout)
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError):
        return {}
    windows: dict[str, int] = {}
    for item in payload.get("data", []):
        if isinstance(item, dict) and item.get("id"):
            windows[item["id"]] = int(item.get("max_model_len") or 0)
    return windows


def _normalize_model_name(name: str) -> str:
    return name.replace(":", "-").strip()


def resolve_llm_model(base_url: Optional[str] = None, timeout: float = 2.0) -> str:
    """Resolve SL_LLM_MODEL against the server model list."""
    load_env()
    configured = llm_model()
    available = fetch_available_models(base_url, timeout=timeout)
    if not available:
        return configured

    if configured in available:
        return configured

    by_lower = {model.lower(): model for model in available}
    candidates = [
        configured,
        _normalize_model_name(configured),
        configured.replace(":", "-").replace("b", "B"),
    ]
    for candidate in candidates:
        if candidate in available:
            return candidate
        if candidate.lower() in by_lower:
            return by_lower[candidate.lower()]

    raise LlmConfigError(
        f"SL_LLM_MODEL={configured!r} is not served at {base_url or llm_base_url()}. "
        f"Available models: {', '.join(available)}"
    )


def ensure_model_context(
    model_id: str,
    *,
    min_tokens: int = 8000,
    base_url: Optional[str] = None,
    timeout: float = 2.0,
) -> None:
    """Fail fast when the selected model context window is too small for skill migrations."""
    windows = fetch_model_context_windows(base_url, timeout=timeout)
    context = windows.get(model_id, 0)
    if context and context < min_tokens:
        alternatives = [
            mid for mid, size in sorted(windows.items(), key=lambda item: item[1], reverse=True)
            if size >= min_tokens
        ]
        hint = f" Suggested models: {', '.join(alternatives[:3])}." if alternatives else ""
        raise LlmConfigError(
            f"SL_LLM_MODEL={model_id!r} has max context {context} tokens; "
            f"migrations need at least {min_tokens}.{hint}"
        )


def llm_reachable(base_url: Optional[str] = None, timeout: float = 2.0) -> bool:
    """Return True if an OpenAI-compatible /models endpoint responds."""
    import urllib.error

    try:
        _fetch_models_payload(base_url, timeout=timeout)
        return True
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError):
        return False


def is_agent_error_response(text: str) -> bool:
    """Return True when agent output looks like a provider/runtime failure."""
    if not text or not text.strip():
        return True
    lowered = text.lower()
    markers = (
        "not found",
        "prompt too long",
        "exceeds max context",
        "error in agent run",
        "api status error",
        "invalid_request_error",
    )
    return any(marker in lowered for marker in markers)
