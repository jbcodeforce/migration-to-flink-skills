from pathlib import Path

from flink_skill_common.compare import compare_files_unordered


def test_compare_files_unordered_self_match(tmp_path: Path):
    ref = tmp_path / "ref.sql"
    ref.write_text("CREATE TABLE t (id STRING);\nINSERT INTO t SELECT 1;\n")
    copy = tmp_path / "copy.sql"
    copy.write_text(ref.read_text())
    result = compare_files_unordered(ref, copy)
    assert result["match_percentage"] == 100.0
    assert result["all_reference_lines_present"]
