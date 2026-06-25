"""Register the Flink sqlglot dialect for offline SQL validation."""

from flink_skill_common.sqlglot_flink.dialect import Flink
from flink_skill_common.sqlglot_flink.expressions import MetadataColumnConstraint, Watermark

__all__ = ["Flink", "MetadataColumnConstraint", "Watermark"]
