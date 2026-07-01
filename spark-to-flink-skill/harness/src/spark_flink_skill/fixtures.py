"""C360 Spark → Flink golden fixture pairs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from spark_flink_skill.config import c360_flink_root, c360_spark_root


@dataclass(frozen=True)
class GoldenPair:
    name: str
    source_file: Path
    flink_ddl: Path
    flink_dml: Path
    table_name: str


def _assert_fixtures_exist(pairs: Iterable[GoldenPair]) -> None:
    """Raise FileNotFoundError if any golden fixture path is missing."""
    missing = []
    for pair in pairs:
        for path in (pair.source_file, pair.flink_ddl, pair.flink_dml):
            if not path.exists():
                missing.append(str(path))
    if missing:
        raise FileNotFoundError("Missing fixture files:\n" + "\n".join(missing))


def c360_golden_pairs() -> list[GoldenPair]:
    """Return registered c360 input/output golden pairs."""
    spark = c360_spark_root()
    flink = c360_flink_root() / "pipelines"

    pairs = [
        GoldenPair(
            name="src_customers",
            source_file=spark / "sources/src_customers.sql",
            flink_ddl=flink / "sources/c360/src_customers/sql-scripts/ddl.src_c360_customers.sql",
            flink_dml=flink / "sources/c360/src_customers/sql-scripts/dml.src_c360_customers.sql",
            table_name="src_c360_customers",
        ),
        GoldenPair(
            name="src_loyalty_program",
            source_file=spark / "sources/src_loyalty_program.sql",
            flink_ddl=flink / "sources/c360/src_loyalty_program/sql-scripts/ddl.src_c360_loyalty_program.sql",
            flink_dml=flink / "sources/c360/src_loyalty_program/sql-scripts/dml.src_c360_loyalty_program.sql",
            table_name="src_c360_loyalty_program",
        ),
        GoldenPair(
            name="src_transactions",
            source_file=spark / "sources/src_transactions.sql",
            flink_ddl=flink / "sources/c360/src_transactions/sql-scripts/ddl.src_c360_transactions.sql",
            flink_dml=flink / "sources/c360/src_transactions/sql-scripts/dml.src_c360_transactions.sql",
            table_name="src_c360_transactions",
        ),
        GoldenPair(
            name="dim_customer_transactions",
            source_file=spark / "intermediates/int_customer_transactions.sql",
            flink_ddl=flink / "dimensions/c360/dim_customer_transactions/sql-scripts/ddl.dim_c360_customer_transactions.sql",
            flink_dml=flink / "dimensions/c360/dim_customer_transactions/sql-scripts/dml.dim_c360_customer_transactions.sql",
            table_name="dim_c360_customer_transactions",
        ),
        GoldenPair(
            name="fct_customer_360_profile",
            source_file=spark / "facts/fct_customer_360_profile.sql",
            flink_ddl=flink / "facts/c360/fct_customer_360_profile/sql-scripts/ddl.c360_fct_customer_profile.sql",
            flink_dml=flink / "facts/c360/fct_customer_360_profile/sql-scripts/dml.c360_fct_customer_profile.sql",
            table_name="c360_fct_customer_profile",
        ),
    ]
    return pairs


def assert_c360_fixtures_exist() -> None:
    _assert_fixtures_exist(c360_golden_pairs())


assert_fixtures_exist = assert_c360_fixtures_exist
