"""
Integration tests for deploying flink statements using a manifest.
"""
import pytest
from pathlib import Path
from cc_deploy.deploy_flink_statements import load_dotenv_file, deploy_flink_statements
from cc_deploy.flink_deploy import get_config, full_undeploy
from manifest.manifest import create_manifest_from_folder, load_manifest
REPO_ROOT = Path(__file__).resolve().parents[3]
MULTI_JOINS = REPO_ROOT / "references" / "flink" / "valid" / "joins" / "multi-joins"

@pytest.fixture
def manifest_path() -> Path:
    return MULTI_JOINS / "deploy_manifest.json" 


def test_deploy_flink_statements(manifest_path: Path) -> None:
    load_dotenv_file()
    config = get_config()
    manifest = create_manifest_from_folder(MULTI_JOINS, overwrite=True)
    deploy_flink_statements(manifest, group="all", sql_dir=MULTI_JOINS, config=config)

def test_undeploy_all_pipeline(manifest_path: Path) -> None:
    load_dotenv_file()
    config = get_config()
    manifest = load_manifest(manifest_path)
    full_undeploy(
                manifest,
                config=config,
                drop_tables_after=True
            )
