"""Shared pytest fixtures."""

from __future__ import annotations

import spark_flink_skill.config  # noqa: F401 — configure shared harness context

import pytest

from spark_flink_skill.fixtures import GoldenPair, c360_golden_pairs
