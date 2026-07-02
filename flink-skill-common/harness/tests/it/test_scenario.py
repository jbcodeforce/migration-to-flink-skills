"""Smoke integration tests for Flink deploy connectivity."""

from pathlib import Path
import pytest

from flink_skill_common.config import (
    flink_deploy_settings, 
    configure, 
    HarnessContext,
    llm_base_url,
)
from flink_skill_common.deploy.flink_statement_manager import FlinkStatementManager
from flink_skill_common.cli_validate import remote
from flink_skill_common.agents.factory import fetch_available_models

__COMMON_ROOT = Path(__file__).resolve().parents[3]
__PROJECT_ROOT = __COMMON_ROOT.parent
configure(HarnessContext(harness_root=__COMMON_ROOT, project_root=__PROJECT_ROOT))

pytestmark = pytest.mark.integration

_REFERENCES_ROOT = __PROJECT_ROOT / "references"

def test_validate_config(require_deploy):
    """Validate Flink deploy settings are present."""
    config = flink_deploy_settings()
    assert config.flink_api_key is not None
    assert config.flink_api_secret is not None
    assert config.organization_id is not None
    assert config.environment_id is not None
    assert config.compute_pool_id is not None
    assert config.database_name is not None
    assert config.endpoint is not None
    assert config.cloud_provider is not None
    assert config.cloud_region is not None
    assert config.poll_seconds is not None
    assert config.timeout_seconds is not None
    assert config.http_user_agent is not None


def test_list_statements(require_deploy):
    """List statements in the Flink environment."""
    manager = FlinkStatementManager()
    result = manager.list_statements()
    statements = result["statements"]
    count = result["count"]
    assert len(statements) == count
    assert count >= 0


def test_list_models(require_llm):
    llm_url = llm_base_url()
    models = fetch_available_models(base_url=llm_url)
    assert len(models) > 0
    print(models)
    assert "Ornith-1.0-9B-6bit" in models
    


def test_validate_good_flink_sql(require_deploy):
    """Validate good Flink SQL."""
    ddl_path = _REFERENCES_ROOT /  Path("flink/valid/routing/filtering/ddl.filtered_pub.sql")
    test_ddl = _REFERENCES_ROOT /  Path("flink/valid/routing/filtering/tests/insert_publications.sql")
    dml_path = _REFERENCES_ROOT /  Path("flink/valid/routing/filtering/dml.filtered_pub.sql")
    try:    
        remote([ddl_path, test_ddl], [dml_path])
    except Exception as e:
        print(e)
        assert False
    finally:
        manager = FlinkStatementManager()
        manager.drop_table("all_publications")
        manager.drop_table("filtered_publications")
   

def test_validate_bad_flink_sql(require_deploy):
    """Validate bad Flink SQL."""
    ddl_path = _REFERENCES_ROOT /  Path("flink/invalid/multi_error_convergence/ddl.validated_song.sql")
    test_ddl = _REFERENCES_ROOT /  Path("flink/invalid/multi_error_convergence/source.sql")
    dml_path = _REFERENCES_ROOT /  Path("flink/invalid/multi_error_convergence/dml.validated_song.sql")
    failed = False
    try:    
        remote([ddl_path, test_ddl], [dml_path])
    except Exception as e:
        print(e)
        failed = True
    finally:
        manager = FlinkStatementManager()
        manager.drop_table("validated_songs")
        manager.drop_table("raw_classical_songs")
    assert not failed

def test_fix_bad_sql_with_agent(require_deploy):
    """Fix bad SQL with agent."""
    ddl_path = _REFERENCES_ROOT /  Path("flink/invalid/multi_error_convergence/ddl.validated_song.sql")
    test_ddl = _REFERENCES_ROOT /  Path("flink/invalid/multi_error_convergence/source.sql")
    dml_path = _REFERENCES_ROOT /  Path("flink/invalid/multi_error_convergence/dml.validated_song.sql")
    try:    
        remote([ddl_path, test_ddl], [dml_path])
    except Exception as e:
        print(e)
        assert False
    finally:
        manager = FlinkStatementManager()