from pathlib import Path
import os

import flink_skill_common.config as config_module
import pytest

from flink_skill_common.config import (
    FlinkDeployNotReadyError,
    FlinkDeploySettings,
    HarnessContext,
    agent_fixer_enabled,
    agent_fixer_max_retries,
    cli_log_file,
    cli_log_level,
    configure,
    dotenv_path,
    flink_api_key,
    flink_api_secret,
    flink_catalog_name,
    flink_compute_pool_id,
    flink_database_name,
    flink_deploy_poll_seconds,
    flink_deploy_settings,
    flink_deploy_timeout_seconds,
    flink_env_id,
    flink_org_id,
    flink_rest_endpoint,
    get_context,
    get_logger,
    llm_api_key,
    llm_base_url,
    llm_model,
    llm_timeout,
    load_env,
    resolve_dotenv_path,
    skill_dir,
    skill_md_path,
)

__COMMON_ROOT = Path(__file__).resolve().parents[2]
__PROJECT_ROOT = __COMMON_ROOT.parent
_HARNESS = HarnessContext(harness_root=__COMMON_ROOT, project_root=__PROJECT_ROOT)
configure(_HARNESS)


def _make_ctx(tmp_path: Path) -> HarnessContext:
    code_root = tmp_path / "repo"
    project_root = code_root / "flink-skill-common"
    harness_root = project_root / "harness"
    harness_root.mkdir(parents=True)
    return HarnessContext(harness_root=harness_root, project_root=project_root)


def _set_complete_flink_deploy_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLINK_API_KEY", "api-key")
    monkeypatch.setenv("FLINK_API_SECRET", "api-secret")
    monkeypatch.setenv("FLINK_ORG_ID", "org-1")
    monkeypatch.setenv("FLINK_ENV_ID", "env-1")
    monkeypatch.setenv("FLINK_COMPUTE_POOL_ID", "lfcp-abc")
    monkeypatch.setenv("FLINK_DATABASE_NAME", "db-1")


def test_config():
    assert _HARNESS == get_context()


def test_get_context_raises_when_not_configured(monkeypatch):
    monkeypatch.setattr(config_module, "_ctx", None)
    with pytest.raises(RuntimeError, match="configure\\(\\)"):
        get_context()
    configure(_HARNESS)


def test_skill_dir():
    assert "flink-skill-common/skill" in str(_HARNESS.skill_dir.as_posix())
    assert "flink-skill-common/skill/SKILL.md" in str(_HARNESS.skill_md_path.as_posix())
    assert "migration-to-flink-skills" in str(_HARNESS.code_root.as_posix())
    assert _HARNESS.harness_root.as_posix() == __COMMON_ROOT.as_posix()
    assert skill_dir() == _HARNESS.skill_dir
    assert skill_md_path() == _HARNESS.skill_md_path


def test_logger():
    assert get_logger() is not None
    assert "logs" in str(cli_log_file().as_posix())
    assert cli_log_file().is_file()


def test_llm_defaults(monkeypatch):
    monkeypatch.delenv("SL_LLM_BASE_URL", raising=False)
    monkeypatch.delenv("SL_LLM_MODEL", raising=False)
    monkeypatch.delenv("SL_LLM_API_KEY", raising=False)
    monkeypatch.delenv("SL_LLM_TIMEOUT", raising=False)

    assert llm_base_url() == "http://localhost:7999/v1"
    assert llm_model() == "qwen3-coder-30b-a3b-instruct-mlx-4bit"
    assert llm_api_key() == "no_llm_key"
    assert llm_timeout() == 10.0


def test_llm_env_overrides(monkeypatch):
    monkeypatch.setenv("SL_LLM_BASE_URL", "http://llm.example/v1")
    monkeypatch.setenv("SL_LLM_MODEL", "custom-model")
    monkeypatch.setenv("SL_LLM_API_KEY", "secret")
    monkeypatch.setenv("SL_LLM_TIMEOUT", "42")

    assert llm_base_url() == "http://llm.example/v1"
    assert llm_model() == "custom-model"
    assert llm_api_key() == "secret"
    assert llm_timeout() == 42.0


def test_flink_deploy_timing_defaults(monkeypatch):
    monkeypatch.delenv("FLINK_DEPLOY_POLL_SECONDS", raising=False)
    monkeypatch.delenv("MCP_DEPLOY_POLL_SECONDS", raising=False)
    monkeypatch.delenv("FLINK_DEPLOY_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("MCP_DEPLOY_TIMEOUT_SECONDS", raising=False)

    assert flink_deploy_poll_seconds() == 5.0
    assert flink_deploy_timeout_seconds() == 300.0


def test_flink_deploy_timing_legacy_aliases(monkeypatch):
    monkeypatch.delenv("FLINK_DEPLOY_POLL_SECONDS", raising=False)
    monkeypatch.delenv("FLINK_DEPLOY_TIMEOUT_SECONDS", raising=False)
    monkeypatch.setenv("MCP_DEPLOY_POLL_SECONDS", "7")
    monkeypatch.setenv("MCP_DEPLOY_TIMEOUT_SECONDS", "120")

    assert flink_deploy_poll_seconds() == 7.0
    assert flink_deploy_timeout_seconds() == 120.0


@pytest.mark.parametrize(
    ("primary", "fallback", "expected"),
    [
        ("FLINK_ORG_ID", "ORGANIZATION_ID", "from-primary"),
        (None, "ORGANIZATION_ID", "from-org"),
        (None, "ORG_ID", "from-org-id"),
    ],
)
def test_flink_org_id_aliases(monkeypatch, primary, fallback, expected):
    monkeypatch.delenv("FLINK_ORG_ID", raising=False)
    monkeypatch.delenv("ORGANIZATION_ID", raising=False)
    monkeypatch.delenv("ORG_ID", raising=False)
    if primary:
        monkeypatch.setenv(primary, expected)
    if fallback:
        monkeypatch.setenv(fallback, expected)
    assert flink_org_id() == expected


@pytest.mark.parametrize(
    ("primary", "fallback", "expected"),
    [
        ("FLINK_ENV_ID", "CC_ENV_ID", "env-primary"),
        (None, "ENVIRONMENT_ID", "env-fallback"),
    ],
)
def test_flink_env_id_aliases(monkeypatch, primary, fallback, expected):
    monkeypatch.delenv("FLINK_ENV_ID", raising=False)
    monkeypatch.delenv("CC_ENV_ID", raising=False)
    monkeypatch.delenv("ENVIRONMENT_ID", raising=False)
    if primary:
        monkeypatch.setenv(primary, expected)
    if fallback:
        monkeypatch.setenv(fallback, expected)
    assert flink_env_id() == expected


def test_flink_compute_pool_id_aliases(monkeypatch):
    monkeypatch.delenv("FLINK_COMPUTE_POOL_ID", raising=False)
    monkeypatch.delenv("CPOOLID", raising=False)
    monkeypatch.setenv("CPOOLID", "lfcp-alias")
    assert flink_compute_pool_id() == "lfcp-alias"


def test_flink_catalog_name_aliases(monkeypatch):
    monkeypatch.delenv("FLINK_CATALOG_NAME", raising=False)
    monkeypatch.delenv("FLINK_ENV_NAME", raising=False)
    monkeypatch.setenv("FLINK_ENV_NAME", "catalog-alias")
    assert flink_catalog_name() == "catalog-alias"


def test_flink_api_credentials_aliases(monkeypatch):
    monkeypatch.delenv("FLINK_API_KEY", raising=False)
    monkeypatch.delenv("CONFLUENT_CLOUD_API_KEY", raising=False)
    monkeypatch.delenv("FLINK_API_SECRET", raising=False)
    monkeypatch.delenv("CONFLUENT_CLOUD_API_SECRET", raising=False)
    monkeypatch.setenv("CONFLUENT_CLOUD_API_KEY", "cloud-key")
    monkeypatch.setenv("CONFLUENT_CLOUD_API_SECRET", "cloud-secret")

    assert flink_api_key() == "cloud-key"
    assert flink_api_secret() == "cloud-secret"


def test_flink_database_name(monkeypatch):
    monkeypatch.setenv("FLINK_DATABASE_NAME", "my-db")
    assert flink_database_name() == "my-db"


def test_flink_rest_endpoint_strips_trailing_slash(monkeypatch):
    monkeypatch.delenv("FLINK_BASE_URL", raising=False)
    monkeypatch.setenv("FLINK_REST_ENDPOINT", "https://flink.example.com/")
    assert flink_rest_endpoint() == "https://flink.example.com"


def test_flink_rest_endpoint_base_url_alias(monkeypatch):
    monkeypatch.delenv("FLINK_REST_ENDPOINT", raising=False)
    monkeypatch.setenv("FLINK_BASE_URL", "https://base.example/")
    assert flink_rest_endpoint() == "https://base.example"


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "YeS"])
def test_agent_fixer_enabled_truthy(monkeypatch, value):
    monkeypatch.setenv("AGENT_FIXER_EXECUTION_ENABLED", value)
    assert agent_fixer_enabled() is True


def test_agent_fixer_enabled_false_by_default(monkeypatch):
    monkeypatch.delenv("AGENT_FIXER_EXECUTION_ENABLED", raising=False)
    assert agent_fixer_enabled() is False


def test_agent_fixer_max_retries_override(monkeypatch):
    monkeypatch.setenv("AGENT_FIXER_EXECUTION_MAX_RETRIES", "5")
    assert agent_fixer_max_retries() == 5


def test_cli_log_level_default_and_override(monkeypatch):
    monkeypatch.delenv("FLINK_LOG_LEVEL", raising=False)
    assert cli_log_level() == "DEBUG"
    monkeypatch.setenv("FLINK_LOG_LEVEL", "warning")
    assert cli_log_level() == "WARNING"


def test_cli_log_file_default():
    path = cli_log_file()
    assert path == _HARNESS.harness_root / "logs" / "ksql-flink-cli.log"


def test_cli_log_file_relative_override(monkeypatch):
    monkeypatch.setenv("FLINK_LOG_FILE", "custom/cli.log")
    path = cli_log_file()
    assert path == (_HARNESS.harness_root / "custom/cli.log").resolve()


def test_cli_log_file_absolute_override(monkeypatch, tmp_path):
    absolute = tmp_path / "absolute.log"
    monkeypatch.setenv("FLINK_LOG_FILE", str(absolute))
    assert cli_log_file() == absolute.resolve()


def test_flink_deploy_settings_success(monkeypatch):
    _set_complete_flink_deploy_env(monkeypatch)
    monkeypatch.setenv("FLINK_REST_ENDPOINT", "https://flink.example/")
    monkeypatch.setenv("CLOUD_PROVIDER", "gcp")
    monkeypatch.setenv("CLOUD_REGION", "europe-west1")
    monkeypatch.setenv("FLINK_DEPLOY_POLL_SECONDS", "3")
    monkeypatch.setenv("FLINK_DEPLOY_TIMEOUT_SECONDS", "90")

    settings = flink_deploy_settings()

    assert isinstance(settings, FlinkDeploySettings)
    assert settings.flink_api_key == "api-key"
    assert settings.flink_api_secret == "api-secret"
    assert settings.organization_id == "org-1"
    assert settings.environment_id == "env-1"
    assert settings.compute_pool_id == "lfcp-abc"
    assert settings.database_name == "db-1"
    assert settings.endpoint == "https://flink.example"
    assert settings.cloud_provider == "gcp"
    assert settings.cloud_region == "europe-west1"
    assert settings.poll_seconds == 3.0
    assert settings.timeout_seconds == 90.0
    assert settings.http_user_agent == "flink-skill-common/0.1"


def test_flink_deploy_settings_missing_raises(monkeypatch):
    for key in (
        "FLINK_API_KEY",
        "FLINK_API_SECRET",
        "FLINK_ORG_ID",
        "FLINK_ENV_ID",
        "FLINK_COMPUTE_POOL_ID",
        "FLINK_DATABASE_NAME",
        "CONFLUENT_CLOUD_API_KEY",
        "CONFLUENT_CLOUD_API_SECRET",
        "ORGANIZATION_ID",
        "ORG_ID",
        "CC_ENV_ID",
        "ENVIRONMENT_ID",
        "CPOOLID",
    ):
        monkeypatch.delenv(key, raising=False)

    with pytest.raises(FlinkDeployNotReadyError, match="FLINK_API_KEY"):
        flink_deploy_settings()

def test_agent_settings_from_shared_dotenv():
    if dotenv_path() is None:
        pytest.skip("No shared .env at repo root; copy .env.example to .env")

    assert flink_org_id() is not None
    assert flink_env_id() is not None
    assert flink_compute_pool_id() is not None
    assert flink_catalog_name() is not None
    assert flink_database_name() is not None
    assert flink_api_key() is not None
    assert flink_api_secret() is not None
    assert flink_rest_endpoint() is not None


def test_resolve_dotenv_default_code_root(tmp_path, monkeypatch):
    monkeypatch.delenv("DOTENV_FILE", raising=False)
    ctx = _make_ctx(tmp_path)
    env_file = ctx.code_root / ".env"
    env_file.write_text("KEY=value\n")
    assert resolve_dotenv_path(ctx) == env_file


def test_resolve_dotenv_missing_file(tmp_path, monkeypatch):
    monkeypatch.delenv("DOTENV_FILE", raising=False)
    ctx = _make_ctx(tmp_path)
    assert resolve_dotenv_path(ctx) is None


def test_resolve_dotenv_absolute_path(tmp_path, monkeypatch):
    external = tmp_path / "external.env"
    external.write_text("KEY=value\n")
    monkeypatch.setenv("DOTENV_FILE", str(external))
    ctx = _make_ctx(tmp_path)
    assert resolve_dotenv_path(ctx) == external


def test_resolve_dotenv_relative_to_code_root(tmp_path, monkeypatch):
    ctx = _make_ctx(tmp_path)
    relative = ctx.code_root / "config" / "shared.env"
    relative.parent.mkdir(parents=True)
    relative.write_text("KEY=value\n")
    monkeypatch.setenv("DOTENV_FILE", "config/shared.env")
    assert resolve_dotenv_path(ctx) == relative.resolve()


def test_dotenv_file_takes_precedence_over_default(tmp_path, monkeypatch):
    ctx = _make_ctx(tmp_path)
    default = ctx.code_root / ".env"
    default.write_text("KEY=default\n")
    external = tmp_path / "override.env"
    external.write_text("KEY=override\n")
    monkeypatch.setenv("DOTENV_FILE", str(external))
    assert resolve_dotenv_path(ctx) == external


def test_load_env_reads_dotenv_file(tmp_path, monkeypatch):
    monkeypatch.delenv("DOTENV_FILE", raising=False)
    ctx = _make_ctx(tmp_path)
    env_file = ctx.code_root / ".env"
    env_file.write_text("DOTENV_TEST_VAR=from-dotenv\n")
    configure(ctx)
    assert load_env() is True
    assert os.getenv("DOTENV_TEST_VAR") == "from-dotenv"
    configure(_HARNESS)


def test_load_env_returns_false_when_missing(tmp_path, monkeypatch):
    monkeypatch.delenv("DOTENV_FILE", raising=False)
    ctx = _make_ctx(tmp_path)
    configure(ctx)
    assert load_env() is False
    configure(_HARNESS)


def test_dotenv_path_accessor(tmp_path, monkeypatch):
    monkeypatch.delenv("DOTENV_FILE", raising=False)
    ctx = _make_ctx(tmp_path)
    env_file = ctx.code_root / ".env"
    env_file.write_text("KEY=value\n")
    configure(ctx)
    assert dotenv_path() == env_file
    configure(_HARNESS)
