"""Agno agent construction helpers for migration skills."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path

from agno.agent import Agent, RunEvent
from agno.models.openai import OpenAIChat
from agno.skills import LocalSkills, Skills


def make_openai_model(*, base_url: str, api_key: str, model_id: str) -> OpenAIChat:
    return OpenAIChat(id=model_id, base_url=base_url, api_key=api_key)


def build_migration_agent(
    *,
    name: str,
    skill_dir: Path | None = None,
    instructions: list[str],
    model: OpenAIChat,
    tools: Sequence[Callable[..., str]] | None = None,
) -> Agent:
    """Create Agno agent with skill loaded from skill_dir."""
    agent_tools = list(tools) if tools else []
    if skill_dir is not None:
        return Agent(
            name=name,
            model=model,
            skills=Skills(loaders=[LocalSkills(str(skill_dir), validate=False)]),
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


def _tool_name(chunk) -> str:
    tool = getattr(chunk, "tool", None)
    if tool is None:
        return "unknown"
    return getattr(tool, "tool_name", None) or str(tool)


def run_agent_response(
    agent: Agent,
    prompt: str,
    *,
    on_event: Callable[[str], None] | None = None,
) -> str:
    """Run agent and return response content as string."""
    if on_event is None:
        response = agent.run(prompt)
        return str(response.content) if hasattr(response, "content") else str(response)

    stream = agent.run(prompt, stream=True, stream_events=True)
    content_parts: list[str] = []
    final_content: str | None = None

    for chunk in stream:
        event = getattr(chunk, "event", None)
        if event == RunEvent.run_started:
            on_event("Agent run started")
        elif event == RunEvent.tool_call_started:
            on_event(f"Tool: {_tool_name(chunk)}")
        elif event == RunEvent.tool_call_completed:
            on_event(f"Tool completed: {_tool_name(chunk)}")
        elif event == RunEvent.run_completed:
            on_event("Agent run completed")
            content = getattr(chunk, "content", None)
            if isinstance(content, str) and content:
                final_content = content
        elif event == RunEvent.run_content:
            content = getattr(chunk, "content", None)
            if isinstance(content, str) and content:
                content_parts.append(content)

    if final_content:
        return final_content
    return "".join(content_parts)
