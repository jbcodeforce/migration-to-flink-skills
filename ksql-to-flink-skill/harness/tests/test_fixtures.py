from ksql_flink_skill.fixtures import assert_fixtures_exist, ksql_golden_pairs


def test_ksql_pairs_registered():
    assert len(ksql_golden_pairs()) >= 3


def test_ksql_fixture_files_exist():
    assert_fixtures_exist()
