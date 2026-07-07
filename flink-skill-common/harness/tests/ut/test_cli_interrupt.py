"""Unit tests for shared CLI interrupt helpers."""

from unittest.mock import MagicMock, patch

import pytest
import typer

from flink_skill_common.cli_interrupt import (
    MIGRATION_INTERRUPT_EXIT_CODE,
    interruptible_sleep,
    run_typer_app,
)


def test_interruptible_sleep_zero_or_negative_returns_immediately():
    with patch("flink_skill_common.cli_interrupt.time.sleep") as mock_sleep:
        interruptible_sleep(0)
        interruptible_sleep(-1)
    mock_sleep.assert_not_called()


def test_interruptible_sleep_uses_short_chunks():
    with patch("flink_skill_common.cli_interrupt.time.sleep") as mock_sleep:
        with patch(
            "flink_skill_common.cli_interrupt.time.monotonic",
            side_effect=[0.0, 0.1, 0.35, 0.6],
        ):
            interruptible_sleep(0.5, step=0.25)
    assert mock_sleep.call_count == 2
    mock_sleep.assert_any_call(0.25)


def test_interruptible_sleep_propagates_keyboard_interrupt():
    with patch(
        "flink_skill_common.cli_interrupt.time.sleep",
        side_effect=KeyboardInterrupt,
    ):
        with pytest.raises(KeyboardInterrupt):
            interruptible_sleep(1.0)


def test_run_typer_app_exits_130_on_keyboard_interrupt():
    app = MagicMock(side_effect=KeyboardInterrupt)

    with pytest.raises(typer.Exit) as exc_info:
        run_typer_app(app)

    assert exc_info.value.exit_code == MIGRATION_INTERRUPT_EXIT_CODE
    app.assert_called_once()
