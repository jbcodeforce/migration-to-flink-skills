"""Shared pytest fixtures."""

from __future__ import annotations

import pytest

from spark_flink_skill.fixtures import GoldenPair, c360_golden_pairs


@pytest.fixture
def src_customers_pair() -> GoldenPair:
    return next(p for p in c360_golden_pairs() if p.name == "src_customers")
