"""Terminal progress reporting for ksql-flink-migrate."""

from __future__ import annotations

import typer

from flink_skill_common.config import get_logger


class ProgressReporter:
    """Plain-text step progress for the migrate CLI."""

    def _log(self, msg: str) -> None:
        get_logger().info("%s", msg)

    def banner(self, **config: str) -> None:
        typer.echo("ksql-flink-migrate")
        self._log("ksql-flink-migrate")
        for key, value in config.items():
            line = f"  {key}: {value}"
            typer.echo(line)
            self._log(line.strip())
        typer.echo("")

    def header(self, msg: str) -> None:
        typer.echo(f"\n=== {msg} ===")
        self._log(f"=== {msg} ===")

    def step(self, n: int, label: str) -> None:
        line = f"→ {n}. {label}"
        typer.echo(line)
        self._log(line)

    def done(self, n: int, label: str, detail: str = "") -> None:
        suffix = f" ({detail})" if detail else ""
        line = f"✓ {n}. {label}{suffix}"
        typer.echo(line)
        self._log(line)

    def agent_event(self, msg: str) -> None:
        typer.echo(f"    · {msg}")
        self._log(f"agent: {msg}")

    def sub(self, msg: str) -> None:
        typer.echo(f"    {msg}")
        self._log(msg.strip())

    def warn(self, msg: str) -> None:
        typer.echo(f"    ! {msg}")
        get_logger().warning("%s", msg)
