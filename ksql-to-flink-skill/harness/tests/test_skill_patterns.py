"""Offline checks that skill documents required translation patterns."""

from ksql_flink_skill.config import skill_dir


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
