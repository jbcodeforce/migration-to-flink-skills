"""
Copyright 2024-2026 Confluent, Inc.
"""
from pathlib import Path

from flink_skill_common.config import (
    get_logger, 
    configure, 
    HarnessContext,
    load_env,
    llm_reachable
)
from flink_skill_common.agents.factory import (
    build_skilled_agent,
    make_openai_model,
    run_agent_process_response,
    resolve_llm_model
)

from flink_skill_common.config import (
    llm_api_key,
    llm_base_url,
    skill_dir,
)
from flink_skill_common.cli_progress import ProgressReporter

_HARNESS_DIR = Path(__file__).resolve().parents[1]
_SKILL_PACKAGE_ROOT = _HARNESS_DIR.parent
_PROJECT_ROOT = _SKILL_PACKAGE_ROOT.parent
configure(
    HarnessContext(
        harness_root=_SKILL_PACKAGE_ROOT,
        project_root=_PROJECT_ROOT,
    )
)

def build_qa_agent():
    """
    Create a flink 101 question and answer agent
    """
    return build_skilled_agent(
        name="FlinkQaAgent",
        skill_dirs=[skill_dir()],
        instructions=[
            "Help answering user questions using the skill `flink-qa`"
            ],
        model=make_openai_model(
                    base_url=llm_base_url(),
                    api_key=llm_api_key(),
                    model_id=resolve_llm_model(),
                ),
        tools=[],
    )

def main() -> None:
    get_logger()
    load_env()
    agent = build_qa_agent()
    done = False
    progress = ProgressReporter()
    if not llm_reachable():
        base = llm_base_url()
        print(f"Error LLM not reachable at {base}")
        exit(1)
    while not done:
        print("Question >:")
        question = input()
        if not question or 'bye' in question:
            done = True
        else:
            print('responding...')
            response=run_agent_process_response(
                agent,
                question,
                on_event=progress.agent_event,
            )
            print(response)


if __name__ == "__main__":
    main() 