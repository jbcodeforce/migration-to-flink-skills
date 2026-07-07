"""
Shared Ctrl-C handling for migration CLIs, so user
can use control-C to interrupt the migration.
"""

from __future__ import annotations

import time
import typer

MIGRATION_INTERRUPT_EXIT_CODE = 130


def interruptible_sleep(seconds: float, *, step: float = 0.25) -> None:
    """Sleep in short chunks so SIGINT is handled promptly between polls."""
    if seconds <= 0:
        return
    deadline = time.monotonic() + seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(step, remaining))


def run_typer_app(app: typer.Typer) -> None:
    try:
        app()
    except KeyboardInterrupt:
        typer.echo("\nMigration interrupted.", err=True)
        raise typer.Exit(MIGRATION_INTERRUPT_EXIT_CODE) from None
