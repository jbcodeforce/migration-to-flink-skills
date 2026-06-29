"""Unit tests for Agno agent factory helpers."""

from unittest.mock import MagicMock

from agno.agent import RunEvent

from flink_skill_common.agents.factory import run_agent_response


class _FakeTool:
    def __init__(self, tool_name: str):
        self.tool_name = tool_name


def _event(event: RunEvent, **kwargs):
    chunk = MagicMock()
    chunk.event = event
    for key, value in kwargs.items():
        setattr(chunk, key, value)
    return chunk


def test_run_agent_response_without_on_event_uses_blocking_run():
    agent = MagicMock()
    agent.run.return_value = MagicMock(content="blocking response")

    result = run_agent_response(agent, "hello")

    assert result == "blocking response"
    agent.run.assert_called_once_with("hello")


def test_run_agent_response_maps_stream_events_to_callback():
    agent = MagicMock()
    events = [
        _event(RunEvent.run_started),
        _event(RunEvent.tool_call_started, tool=_FakeTool("get_skill_instructions")),
        _event(RunEvent.tool_call_completed, tool=_FakeTool("get_skill_instructions")),
        _event(RunEvent.run_content, content="partial "),
        _event(RunEvent.run_completed, content="final response"),
    ]
    agent.run.return_value = iter(events)

    seen: list[str] = []
    result = run_agent_response(agent, "migrate", on_event=seen.append)

    agent.run.assert_called_once_with("migrate", stream=True, stream_events=True)
    assert seen == [
        "Agent run started",
        "Tool: get_skill_instructions",
        "Tool completed: get_skill_instructions",
        "Agent run completed",
    ]
    assert result == "final response"


def test_run_agent_response_joins_run_content_when_no_completed_content():
    agent = MagicMock()
    events = [
        _event(RunEvent.run_started),
        _event(RunEvent.run_content, content="chunk1"),
        _event(RunEvent.run_content, content="chunk2"),
        _event(RunEvent.run_completed),
    ]
    agent.run.return_value = iter(events)

    result = run_agent_response(agent, "migrate", on_event=lambda _msg: None)

    assert result == "chunk1chunk2"
