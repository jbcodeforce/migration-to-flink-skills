from flink_skill_common.config import configure, HarnessContext
from pathlib import Path

from flink_skill_common.llm import llm_reachable


def test_llm_config():
    configure(HarnessContext(harness_root=Path(__file__).resolve().parents[2], project_root=Path(__file__).resolve().parents[2]))
   
    assert llm_reachable() is True