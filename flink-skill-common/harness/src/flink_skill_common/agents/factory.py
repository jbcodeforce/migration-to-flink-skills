"""Agno agent construction helpers for migration skills."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path

from agno.agent import Agent
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
    else:
        return Agent(
            name=name,
            model=model,
            tools=agent_tools,
            instructions=instructions,
            markdown=True,
        )


def run_agent_response(agent: Agent, prompt: str) -> str:
    """Run agent and return response content as string."""
    response = agent.run(prompt)
    return str(response.content) if hasattr(response, "content") else str(response)
