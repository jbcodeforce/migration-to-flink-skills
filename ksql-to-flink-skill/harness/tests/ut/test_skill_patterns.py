"""Offline checks that skill documents required translation patterns."""

from flink_skill_common.config import skill_dir, flink_skill_common_skill_dir


def test_skill_documents_cte_group_by_for_latest_by_offset():
    skill_md = (skill_dir() / "SKILL.md").read_text()
    assert "replaces `EMIT CHANGES`" in skill_md
    assert "tests/" in skill_md

def test_skill_references_confluent_sql_deploy():
    deploy_doc = (flink_skill_common_skill_dir() / "validate-flink-sql" / "references" / "confluent-sql-deploy.md").read_text()
    assert "create_flink_statement" in deploy_doc
    assert "get_flink_statement_exceptions" in deploy_doc
    assert "-ddl" in deploy_doc and "-dml" in deploy_doc
    assert "tests/ddl" in deploy_doc or "tests/" in deploy_doc
