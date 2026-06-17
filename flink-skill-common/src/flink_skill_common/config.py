"""Shared harness configuration and environment accessors."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

_ctx: HarnessContext | None = None


@dataclass(frozen=True)
class HarnessContext:
    """Paths and env loading for a skill harness project."""

    harness_root: Path
    project_root: Path

    def load_env(self) -> None:
        load_dotenv(self.harness_root / ".env", override=False)

    @property
    def skill_dir(self) -> Path:
        return self.project_root / "skill"

    @property
    def skill_md_path(self) -> Path:
        return self.skill_dir / "SKILL.md"

    @property
    def code_root(self) -> Path:
        return self.project_root.parent.parent


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


def get_context() -> HarnessContext:
    if _ctx is None:
        raise RuntimeError(
            "flink_skill_common.config.configure() must be called before using config accessors"
        )
    return _ctx


def load_env() -> None:
    get_context().load_env()


def llm_base_url() -> str:
    load_env()
    return os.getenv("SL_LLM_BASE_URL", "http://localhost:7999/v1")


def llm_model() -> str:
    load_env()
    return os.getenv("SL_LLM_MODEL", "qwen3-coder-30b-a3b-instruct-mlx-4bit")


def llm_api_key() -> str:
    load_env()
    return os.getenv("SL_LLM_API_KEY", "no_llm_key")


def flink_deploy_poll_seconds() -> float:
    load_env()
    raw = os.getenv("FLINK_DEPLOY_POLL_SECONDS") or os.getenv("MCP_DEPLOY_POLL_SECONDS", "5")
    return float(raw)


def flink_deploy_timeout_seconds() -> float:
    load_env()
    raw = os.getenv("FLINK_DEPLOY_TIMEOUT_SECONDS") or os.getenv("MCP_DEPLOY_TIMEOUT_SECONDS", "300")
    return float(raw)


def flink_org_id() -> str | None:
    load_env()
    return os.getenv("FLINK_ORG_ID") or os.getenv("ORGANIZATION_ID") or os.getenv("ORG_ID")


def flink_env_id() -> str | None:
    load_env()
    return os.getenv("FLINK_ENV_ID") or os.getenv("CC_ENV_ID") or os.getenv("ENVIRONMENT_ID") or os.getenv("ENV_ID")


def flink_compute_pool_id() -> str | None:
    load_env()
    return os.getenv("FLINK_COMPUTE_POOL_ID") or os.getenv("CPOOLID")


def flink_catalog_name() -> str | None:
    load_env()
    return os.getenv("FLINK_CATALOG_NAME") or os.getenv("FLINK_ENV_NAME")


def flink_database_name() -> str | None:
    load_env()
    return os.getenv("FLINK_DATABASE_NAME")


def flink_api_key() -> str | None:
    load_env()
    return os.getenv("FLINK_API_KEY") or os.getenv("CONFLUENT_CLOUD_API_KEY")


def flink_api_secret() -> str | None:
    load_env()
    return os.getenv("FLINK_API_SECRET") or os.getenv("CONFLUENT_CLOUD_API_SECRET")


def flink_rest_endpoint() -> str | None:
    load_env()
    raw = os.getenv("FLINK_REST_ENDPOINT") or os.getenv("FLINK_BASE_URL")
    return raw.rstrip("/") if raw else None


def flink_deploy_settings() -> FlinkDeploySettings:
    """Load Flink deploy settings from environment."""
    load_env()
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

