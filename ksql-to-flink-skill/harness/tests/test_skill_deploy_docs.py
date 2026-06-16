"""Offline checks that skill documents confluent-sql deploy workflow."""

from ksql_flink_skill.config import skill_dir


def test_skill_references_confluent_sql_deploy():
    skill_md = (skill_dir() / "SKILL.md").read_text()
    deploy_doc = (skill_dir() / "references/confluent-sql-deploy.md").read_text()

    assert "confluent-sql-deploy.md" in skill_md
    assert "create_flink_statement" in deploy_doc
    assert "get_flink_statement_exceptions" in deploy_doc
    assert "-ddl" in deploy_doc and "-dml" in deploy_doc
    assert "tests/" in skill_md
    assert "tests/ddl" in deploy_doc or "tests/" in deploy_doc
