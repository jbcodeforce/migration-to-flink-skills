"""Offline checks that skill documents required translation patterns."""

from pathlib import Path
_HARNESS_ROOT = Path(__file__).resolve().parents[2]
_PROJECT_ROOT = _HARNESS_ROOT.parent
from flink_skill_common.config import HarnessContext, configure, skill_dir
configure(HarnessContext(harness_root=_HARNESS_ROOT, project_root=_PROJECT_ROOT))


def test_skill_documents_cte_group_by_for_latest_by_offset():
    skill_md = (skill_dir() / "SKILL.md").read_text()
    translator = (skill_dir() / "prompts/ksql_fsql/translator.txt").read_text()
    examples = (skill_dir() / "references/examples.md").read_text()

    for text in (skill_md, translator, examples):
        assert "WITH deduplicated AS" in text
        assert "GROUP BY" in text
        assert "LATEST_BY_OFFSET" in text

    assert "must use `WITH deduplicated AS`" in skill_md or "WITH deduplicated AS" in skill_md
    assert "kma_chat" in examples


def test_skill_references_confluent_sql_deploy():
    skill_md = (skill_dir() / "SKILL.md").read_text()
    deploy_doc = (skill_dir() / "references/confluent-sql-deploy.md").read_text()

    assert "confluent-sql-deploy.md" in skill_md
    assert "create_flink_statement" in deploy_doc
    assert "get_flink_statement_exceptions" in deploy_doc
    assert "-ddl" in deploy_doc and "-dml" in deploy_doc
    assert "tests/" in skill_md
    assert "tests/ddl" in deploy_doc or "tests/" in deploy_doc