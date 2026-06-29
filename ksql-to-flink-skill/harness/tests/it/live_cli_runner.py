"""Typer CliRunner that echoes stdout/stderr to the real terminal while invoke runs."""

from __future__ import annotations

import contextlib
import io
import sys
from collections.abc import Generator, Mapping
from typing import IO, Any

import typer.testing as typer_testing
from typer.testing import CliRunner, StreamMixer


class _TeeBytesIOCopy(io.BytesIO):
    """BytesIO that also copies writes to a live text stream (for pytest -s-free IT output)."""

    def __init__(
        self,
        copy_to: io.BytesIO,
        echo: io.TextIO,
        charset: str,
    ) -> None:
        super().__init__()
        self.copy_to = copy_to
        self.echo = echo
        self.charset = charset

    def flush(self) -> None:
        super().flush()
        self.copy_to.flush()

    def write(self, b: bytes | bytearray | memoryview) -> int:  # type: ignore[override]
        self.copy_to.write(b)
        try:
            self.echo.write(bytes(b).decode(self.charset, errors="replace"))
            self.echo.flush()
        except Exception:
            pass
        return super().write(b)


class _TeeStreamMixer(StreamMixer):
    def __init__(self, charset: str, echo_stdout: io.TextIO, echo_stderr: io.TextIO) -> None:
        self.output = io.BytesIO()
        self.stdout = _TeeBytesIOCopy(self.output, echo_stdout, charset)
        self.stderr = _TeeBytesIOCopy(self.output, echo_stderr, charset)


class LiveCliRunner(CliRunner):
    """Echo CLI output to sys.__stdout__ / sys.__stderr__ while still capturing result.output."""

    @contextlib.contextmanager
    def isolation(
        self,
        input: str | bytes | None = None,
        env: Mapping[str, str | None] | None = None,
        color: bool = False,
    ) -> Generator[tuple[io.BytesIO, io.BytesIO, io.BytesIO], None, None]:
        original_mixer = typer_testing.StreamMixer

        def _live_mixer() -> StreamMixer:
            return _TeeStreamMixer(
                self.charset,
                sys.__stdout__,
                sys.__stderr__,
            )

        typer_testing.StreamMixer = _live_mixer  # type: ignore[assignment]
        try:
            with super().isolation(input=input, env=env, color=color) as streams:
                yield streams
        finally:
            typer_testing.StreamMixer = original_mixer
