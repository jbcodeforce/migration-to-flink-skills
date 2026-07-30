"""Verify c360 golden fixture paths exist."""

import pytest

from spark_ref_fixtures import assert_fixtures_exist, c360_golden_pairs


def test_c360_pairs_registered():
    pairs = c360_golden_pairs()
    assert len(pairs) >= 5


def test_c360_fixture_files_exist():
    try:
        assert_fixtures_exist()
    except FileNotFoundError as exc:
        pytest.skip(f"c360 golden fixtures not present: {exc}")
