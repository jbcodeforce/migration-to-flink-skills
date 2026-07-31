"""Verify spark reference migrate cases resolve to real source files."""

from spark_ref_fixtures import SPARK_MIGRATE_CASES, spark_source_path


def test_spark_cases_registered():
    assert len(SPARK_MIGRATE_CASES) >= 4


def test_spark_source_files_exist():
    for case in SPARK_MIGRATE_CASES:
        assert spark_source_path(case).is_file(), case.rel_path
