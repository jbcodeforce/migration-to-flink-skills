"""Flink-specific sqlglot expression nodes for offline DDL validation."""

from __future__ import annotations

from sqlglot.expressions import Expression


class Watermark(Expression):
    """Table watermark: WATERMARK FOR col AS expr."""

    arg_types = {"column": True, "expression": True}


class MetadataColumnConstraint(Expression):
    """Column metadata: METADATA FROM 'key' or METADATA VIRTUAL."""

    arg_types = {"from_key": False, "virtual": False}
