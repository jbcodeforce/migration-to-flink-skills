"""User-facing error message formatting for CLI output."""

from __future__ import annotations


def format_user_error(exc: BaseException) -> str:
    """Return a concise error message from an exception chain."""
    root = exc
    while root.__cause__ is not None:
        root = root.__cause__

    root_msg = str(root).strip()
    wrapper_msg = str(exc).strip()

    if root is not exc and root_msg:
        return root_msg

    if wrapper_msg.startswith("Failed to create ") and ": " in wrapper_msg:
        inner = wrapper_msg.split(": ", 1)[1].strip()
        if inner:
            return inner

    return root_msg or wrapper_msg or exc.__class__.__name__


def format_agent_retry_message(error: str, attempt: int, max_attempts: int) -> str:
    """Message shown when the agent fixer will retry after a recoverable failure."""
    return (
        f"{error} Agent fixer will attempt to correct SQL automatically "
        f"(attempt {attempt}/{max_attempts})."
    )
