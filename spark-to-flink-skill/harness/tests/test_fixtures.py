"""Verify c360 golden fixture paths exist."""

from spark_flink_skill.fixtures import assert_fixtures_exist, c360_golden_pairs


def test_c360_pairs_registered():
    pairs = c360_golden_pairs()
    assert len(pairs) >= 5


def test_c360_fixture_files_exist():
    assert_fixtures_exist()
