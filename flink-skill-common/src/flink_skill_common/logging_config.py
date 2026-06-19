"""File logging for the ksql-flink CLI."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
import os


_LOGGER = None
_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(filename)s:%(lineno)d %(message)s"


def get_logger() -> logging.Logger:
    if _LOGGER is None:
        configure_cli_logging("flink_migration_skill.cli")
    return _LOGGER

def configure_cli_logging(name: str) -> logging.Logger:
    """Configure file (+ stderr) logging once and return the CLI logger."""

    global _LOGGER
    if _LOGGER:
        return _LOGGER
    logger = logging.getLogger(name or "flink_migration_skill.cli")


    log_path = cli_log_file()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    level = getattr(logging, cli_log_level(), logging.DEBUG)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(logging.Formatter(_LOG_FORMAT))

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.WARNING)
    stderr_handler.setFormatter(logging.Formatter(_LOG_FORMAT))

    logger.setLevel(level)
    logger.handlers.clear()
    logger.addHandler(file_handler)
    logger.addHandler(stderr_handler)
    logger.propagate = False

    _LOGGER = logger
    logger.debug("Logging to %s (level=%s)", log_path, cli_log_level())
    return logger



def cli_log_file() -> Path:
    raw = os.getenv("KSQL_FLINK_LOG_FILE")
    from .config import get_context
    if raw:
        path = Path(raw)
        return path.resolve() if path.is_absolute() else (get_context().harness_root / path).resolve()
    return get_context().harness_root / "logs" / "ksql-flink-cli.log"


def cli_log_level() -> str:
    return os.getenv("KSQL_FLINK_LOG_LEVEL", "DEBUG").upper()
