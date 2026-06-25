"""Custom sqlglot dialect for Confluent Cloud Flink SQL."""

from __future__ import annotations

from sqlglot.dialects.spark import Spark

from flink_skill_common.sqlglot_flink.parser import FlinkParser


class Flink(Spark):
    """Spark-derived dialect with Flink WATERMARK and METADATA column support."""

    Parser = FlinkParser
