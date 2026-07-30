"""Flink SQL parser extensions for Confluent Cloud Flink DDL."""

from __future__ import annotations

from sqlglot import exp
from sqlglot.parsers.spark import SparkParser
from sqlglot.tokens import TokenType

from flink_skill_common.sqlglot_flink.expressions import MetadataColumnConstraint, Watermark


class FlinkParser(SparkParser):
    SCHEMA_UNNAMED_CONSTRAINTS = SparkParser.SCHEMA_UNNAMED_CONSTRAINTS | {"WATERMARK"}

    CONSTRAINT_PARSERS = {
        **SparkParser.CONSTRAINT_PARSERS,
        "WATERMARK": lambda self: self._parse_watermark(),
    }

    def _parse_primary(self) -> exp.Expr | None:
        # Flink window TVFs: TUMBLE(TABLE clicks, DESCRIPTOR(ts), ...)
        index = self._index
        if self._match(TokenType.TABLE):
            if not self._match(TokenType.L_PAREN, advance=False):
                return self._parse_table_parts()
            self._retreat(index)
        return super()._parse_primary()

    def _parse_watermark(self) -> Watermark:
        self._match_text_seq("FOR")
        column = self._parse_column()
        self._match_text_seq("AS")
        expression = self._parse_expression()
        return self.expression(Watermark(column=column, expression=expression))

    def _parse_column_constraint(self) -> exp.ColumnConstraint | None:
        if self._match_text_seq("METADATA", advance=False):
            self._match_text_seq("METADATA")
            from_key = None
            virtual = False
            if self._match_text_seq("FROM"):
                from_key = self._parse_string()
            elif self._match_text_seq("VIRTUAL"):
                virtual = True
            kind = MetadataColumnConstraint(from_key=from_key, virtual=virtual)
            return self.expression(exp.ColumnConstraint(kind=kind))
        return super()._parse_column_constraint()
