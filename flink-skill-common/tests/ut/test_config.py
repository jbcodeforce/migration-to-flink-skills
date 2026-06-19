

from pathlib import Path
from ksql_to_flink.config import skill_dir, skill_md_path, agent_deploy_on_failure, agent_deploy_max_retries, cli_log_file, cli_log_level



def test_config():
    assert agent_deploy_on_failure() == False
    assert agent_deploy_max_retries() == 2
    assert cli_log_level() == "DEBUG"

