"""Terminal progress reporting for ksql-flink-migrate."""

from __future__ import annotations

import typer


class ProgressReporter:
    """Plain-text step progress for the migrate CLI."""

    def banner(self, **config: str) -> None:
        typer.echo("ksql-flink-migrate")
        for key, value in config.items():
            typer.echo(f"  {key}: {value}")
        typer.echo("")

    def header(self, msg: str) -> None:
        typer.echo(f"\n=== {msg} ===")

    def step(self, n: int, label: str) -> None:
        typer.echo(f"→ {n}. {label}")

    def done(self, n: int, label: str, detail: str = "") -> None:
        suffix = f" ({detail})" if detail else ""
        typer.echo(f"✓ {n}. {label}{suffix}")

    def agent_event(self, msg: str) -> None:
        typer.echo(f"    · {msg}")

    def sub(self, msg: str) -> None:
        typer.echo(f"    {msg}")
