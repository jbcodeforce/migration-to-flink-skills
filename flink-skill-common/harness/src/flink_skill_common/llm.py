"""
Copyright 2024-2026 Confluent, Inc.
KSQL to Flink SQL Translation Agent

OpenAI-compatible LLM configuration helpers.
"""

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
