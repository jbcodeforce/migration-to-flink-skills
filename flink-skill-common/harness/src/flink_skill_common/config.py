"""
Copyright 2024-2026 Confluent, Inc.
KSQL to Flink SQL Translation Agent

Shared harness configuration and environment accessors.
"""

from __future__ import annotations

import os, sys
from dataclasses import dataclass
from pathlib import Path
import logging
from dotenv import load_dotenv

_LOGGER = None
_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(filename)s:%(lineno)d %(message)s"

_ctx: HarnessContext | None = None
_DOTENV_ENV_VAR = "DOTENV_FILE"


def resolve_dotenv_path(ctx: HarnessContext) -> Path | None:
    """Resolve the shared .env file path for a harness context."""
    raw = os.getenv(_DOTENV_ENV_VAR)
    if raw:
        path = Path(raw)
        if not path.is_absolute():
            path = (ctx.code_root / path).resolve()
    else:
        path = ctx.code_root / ".env"
    return path if path.is_file() else None


@dataclass(frozen=True)
class HarnessContext:
    """Paths and env loading for a skill harness project."""

    harness_root: Path
    project_root: Path

    def load_env(self) -> bool:
        path = resolve_dotenv_path(self)
        if path is None:
            return False
        return load_dotenv(path, override=True)

    @property
    def skill_dir(self) -> Path:
        return self.project_root / "skill"

    @property
    def skill_md_path(self) -> Path:
        return self.skill_dir / "SKILL.md"

    @property
    def code_root(self) -> Path:
        return self.project_root.parent


@dataclass(frozen=True)
class FlinkDeploySettings:
    """Confluent Cloud Flink deploy credentials and timing."""

    flink_api_key: str
    flink_api_secret: str
    organization_id: str
    environment_id: str
    compute_pool_id: str
    database_name: str
    endpoint: str | None
    cloud_provider: str
    cloud_region: str
    poll_seconds: float
    timeout_seconds: float
    http_user_agent: str = "flink-skill-common/0.1"


class FlinkDeployNotReadyError(RuntimeError):
    """Flink deploy credentials or settings are incomplete."""


def configure(ctx: HarnessContext) -> None:
    """Register the active harness context (call once from skill config module)."""
    global _ctx
    _ctx = ctx
    if ctx.load_env():
        logging.getLogger(__name__).debug(
            "Loaded environment from %s", resolve_dotenv_path(ctx)
        )


def get_context() -> HarnessContext:
    if _ctx is None:
        raise RuntimeError(
            "flink_skill_common.config.configure() must be called before using config accessors"
        )
    return _ctx


def load_env() -> bool:
    return get_context().load_env()


def dotenv_path() -> Path | None:
    return resolve_dotenv_path(get_context())


def llm_base_url() -> str:
    return os.getenv("SL_LLM_BASE_URL", "http://localhost:7999/v1")


def llm_model() -> str:
    return os.getenv("SL_LLM_MODEL", "qwen3-coder-30b-a3b-instruct-mlx-4bit")


def llm_api_key() -> str:
    return os.getenv("SL_LLM_API_KEY", "no_llm_key")


def llm_timeout() -> float:
    return float(os.getenv("SL_LLM_TIMEOUT", "10"))


def flink_deploy_poll_seconds() -> float:
    raw = os.getenv("FLINK_DEPLOY_POLL_SECONDS") or os.getenv("MCP_DEPLOY_POLL_SECONDS", "5")
    return float(raw)


def flink_deploy_timeout_seconds() -> float:
    raw = os.getenv("FLINK_DEPLOY_TIMEOUT_SECONDS") or os.getenv("MCP_DEPLOY_TIMEOUT_SECONDS", "300")
    return float(raw)


def flink_org_id() -> str | None:
    return os.getenv("FLINK_ORG_ID") or os.getenv("ORGANIZATION_ID") or os.getenv("ORG_ID")


def flink_env_id() -> str | None:
    return os.getenv("FLINK_ENV_ID") or os.getenv("CC_ENV_ID") or os.getenv("ENVIRONMENT_ID") or os.getenv("FLINK_ENV_ID")


def flink_compute_pool_id() -> str | None:
    return os.getenv("FLINK_COMPUTE_POOL_ID") or os.getenv("CPOOLID")


def flink_catalog_name() -> str | None:
    return os.getenv("FLINK_CATALOG_NAME") or os.getenv("FLINK_ENV_NAME")


def flink_database_name() -> str | None:
    return os.getenv("FLINK_DATABASE_NAME")


def flink_api_key() -> str | None:
    return os.getenv("FLINK_API_KEY") or os.getenv("CONFLUENT_CLOUD_API_KEY")


def flink_api_secret() -> str | None:
    return os.getenv("FLINK_API_SECRET") or os.getenv("CONFLUENT_CLOUD_API_SECRET")


def flink_rest_endpoint() -> str | None:
    raw = os.getenv("FLINK_REST_ENDPOINT") or os.getenv("FLINK_BASE_URL")
    return raw.rstrip("/") if raw else None



def skill_dir() -> Path:
    return get_context().skill_dir


def skill_md_path() -> Path:
    return get_context().skill_md_path


def flink_skill_common_skill_dir() -> Path:
    """Return flink-skill-common/skill regardless of which harness called configure()."""
    # harness/src/flink_skill_common/config.py → parents[3] is flink-skill-common/
    return Path(__file__).resolve().parents[3] / "skill"


def agent_fixer_enabled() -> bool:
    return os.getenv("AGENT_FIXER_EXECUTION_ENABLED", "").lower() in ("1", "true", "yes")


def agent_fixer_max_retries() -> int:
    return int(os.getenv("AGENT_FIXER_EXECUTION_MAX_RETRIES", "2"))


def flink_deploy_settings() -> FlinkDeploySettings:
    """Load Flink deploy settings from environment."""
    missing: list[str] = []
    api_key = flink_api_key()
    api_secret = flink_api_secret()
    org_id = flink_org_id()
    env_id = flink_env_id()
    pool_id = flink_compute_pool_id()
    database = flink_database_name()

    if not api_key:
        missing.append("FLINK_API_KEY")
    if not api_secret:
        missing.append("FLINK_API_SECRET")
    if not org_id:
        missing.append("FLINK_ORG_ID")
    if not env_id:
        missing.append("FLINK_ENV_ID")
    if not pool_id:
        missing.append("FLINK_COMPUTE_POOL_ID")
    if not database:
        missing.append("FLINK_DATABASE_NAME")
    if missing:
        raise FlinkDeployNotReadyError(
            "Missing Flink deploy environment variables: "
            + ", ".join(missing)
            + ". See docs/FLINK_DEPLOY.md."
        )

    return FlinkDeploySettings(
        flink_api_key=api_key,
        flink_api_secret=api_secret,
        organization_id=org_id,
        environment_id=env_id,
        compute_pool_id=pool_id,
        database_name=database,
        endpoint=flink_rest_endpoint(),
        cloud_provider=os.getenv("CLOUD_PROVIDER", "aws"),
        cloud_region=os.getenv("CLOUD_REGION", "us-west-2"),
        poll_seconds=flink_deploy_poll_seconds(),
        timeout_seconds=flink_deploy_timeout_seconds(),
    )


def get_logger() -> logging.Logger:
    if _LOGGER is None:
        _configure_cli_logging("flink_migration_skill.cli")
    return _LOGGER

def _configure_cli_logging(name: str) -> logging.Logger:
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
    raw = os.getenv("FLINK_LOG_FILE")
    from .config import get_context
    if raw:
        path = Path(raw)
        return path.resolve() if path.is_absolute() else (get_context().harness_root / path).resolve()
    return get_context().harness_root / "logs" / "ksql-flink-cli.log"


def cli_log_level() -> str:
    return os.getenv("FLINK_LOG_LEVEL", "DEBUG").upper()