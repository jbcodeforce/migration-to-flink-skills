"""Agno agent construction helpers for migration agents, and other validation agents."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Optional

from agno.agent import Agent, RunEvent
from agno.models.openai import OpenAIChat
from agno.skills import Skills
from flink_skill_common.agents.skill_loaders import AgnoAdaptedLocalSkills
from flink_skill_common.config import get_logger, llm_model, fetch_models_payload

def make_openai_model(*, base_url: str, api_key: str, model_id: str) -> OpenAIChat:
    return OpenAIChat(id=model_id, base_url=base_url, api_key=api_key)


def resolve_llm_model(base_url: str | None = None, timeout: float | None = None) -> str:
    """Resolve SL_LLM_MODEL against the server model list."""
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

    raise RuntimeError(
        f"SL_LLM_MODEL={configured!r} is not served at {base_url or llm_base_url()}. "
        f"Available models: {', '.join(available)}"
    )


def fetch_available_models(
    base_url: str | None = None, timeout: float | None = None
) -> list[str]:
    """Return model ids from an OpenAI-compatible /models endpoint."""
    return list(fetch_model_context_windows(base_url, timeout=timeout).keys())

def fetch_model_context_windows(
    base_url: Optional[str] = None, timeout: float | None = None
) -> dict[str, int]:
    """Return model id -> max context window from /models metadata."""
    payload = fetch_models_payload(base_url, timeout=timeout)
    if not payload:
        return {}
    windows: dict[str, int] = {}
    for item in payload.get("data", []):
        if isinstance(item, dict) and item.get("id"):
            windows[item["id"]] = int(item.get("max_model_len") or 0)
    return windows
    
def build_skilled_agent(
    *,
    name: str,
    skill_dirs: Sequence[Path] | None = None,
    instructions: list[str],
    model: OpenAIChat,
    tools: Sequence[Callable[..., str]] | None = None,
) -> Agent:
    """
    Create Agno agent with skills loaded from one or more skill directories.
    If skill_dirs is omitted or empty, the agent is created without skills.
    """
    agent_tools = list(tools) if tools else []
    dirs = list(skill_dirs) if skill_dirs else []
    if dirs:
        loaders = [AgnoAdaptedLocalSkills(str(path), validate=False) for path in dirs]
        return Agent(
            name=name,
            model=model,
            skills=Skills(loaders=loaders),
            tools=agent_tools,
            instructions=instructions,
            markdown=True,
        )
    return Agent(
        name=name,
        model=model,
        tools=agent_tools,
        instructions=instructions,
        markdown=True,
    )


def _normalize_model_name(name: str) -> str:
    return name.replace(":", "-").strip()

def _tool_name(chunk) -> str:
    """
    Get the name of the tool that was called, by searching the LLM response.
    """
    tool = getattr(chunk, "tool", None)
    if tool is None:
        return "unknown"
    return getattr(tool, "tool_name", None) or str(tool)


def run_agent_process_response(
    agent: Agent,
    prompt: str,
    *,
    on_event: Callable[[str], None] | None = None,
) -> str:
    """Run agent and return response content as string."""
    logger = get_logger()

    def _emit(msg: str) -> None:
        logger.info("agent: %s", msg)
        if on_event is not None:
            on_event(msg)

    if on_event is None:
        logger.info("agent: starting run (non-streaming)")
        response = agent.run(prompt)
        content = str(response.content) if hasattr(response, "content") else str(response)
        logger.info("agent: run finished (%d chars)", len(content))
        return content

    stream = agent.run(prompt, stream=True, stream_events=True)
    content_parts: list[str] = []
    final_content: str | None = None

    try:
        for chunk in stream:
            event = getattr(chunk, "event", None)
            if event == RunEvent.run_started:
                _emit("Agent run started")
            elif event == RunEvent.tool_call_started:
                _emit(f"Tool: {_tool_name(chunk)}")
            elif event == RunEvent.tool_call_completed:
                _emit(f"Tool completed: {_tool_name(chunk)}")
            elif event == RunEvent.run_completed:
                _emit("Agent run completed")
                content = getattr(chunk, "content", None)
                if isinstance(content, str) and content:
                    final_content = content
            elif event == RunEvent.run_content:
                content = getattr(chunk, "content", None)
                if isinstance(content, str) and content:
                    content_parts.append(content)
    except KeyboardInterrupt:
        _emit("Agent run interrupted")
        raise

    if final_content:
        logger.info("agent: run finished (%d chars)", len(final_content))
        return final_content
    content = "".join(content_parts)
    logger.info("agent: run finished (%d chars)", len(content))
    return content
