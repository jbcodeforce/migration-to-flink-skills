"""Shared helpers for references/spark/c360/sources migration integration tests."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

from flink_skill_common.migration_manifest import statements_dir_for
from live_cli_runner import LiveCliRunner
from spark_to_flink.cli import app

REPO_ROOT = Path(__file__).resolve().parents[3]
SPARK_SOURCES_ROOT = REPO_ROOT / "references" / "spark" / "c360" / "sources"

runner = LiveCliRunner()
output_path = REPO_ROOT / "staging" / "spark2flk"


@dataclass(frozen=True)
class SparkMigrateCase:
    """One Spark SQL source file and its Flink migration target table."""

    rel_path: str
    target_table: str
    category: str
    notes: str = ""


SPARK_MIGRATE_CASES: list[SparkMigrateCase] = [
    # tables
    SparkMigrateCase("tables/src_customer_profiles.sql", "src_customer_profiles", "seed"),
    SparkMigrateCase("tables/src_purchases.sql", "src_purchases", "seed"),
    SparkMigrateCase("tables/src_web_events.sql", "src_web_events", "seed"),
    # users
    SparkMigrateCase("users/raw_active_users.sql", "raw_active_users", "users"),
    SparkMigrateCase("src_set_operations.sql", "src_coperations", "c360"),
    SparkMigrateCase("src_streaming_aggregations.sql", "src_streaming_aggregations", "c360"),
    SparkMigrateCase("src_temporal_analytics.sql", "src_temporal_analytics", "c360"), 
    SparkMigrateCase("src_advanced_transformations.sql", "src_advanced_transformations", "c360"),
    SparkMigrateCase("src_customer_journey.sql", "src_customer_journey", "c360"),
    SparkMigrateCase("src_event_processing.sql", "src_event_processing", "c360"),
    SparkMigrateCase("src_product_analytics.sql", "src_product_analytics", "c360"),
    SparkMigrateCase("src_sales_pivot.sql", "src_sales_pivot", "c360"),
    SparkMigrateCase("src_sales_pivot.sql", "src_sales_pivot", "c360"),

]

SPARK_EXCLUDED_SOURCES: list[tuple[str, str]] = [
    ("users/raw_inactive_users.sql", "INSERT seed data only"),
]


def spark_source_path(case: SparkMigrateCase) -> Path:
    path = SPARK_SOURCES_ROOT / case.rel_path
    if not path.is_file():
        raise FileNotFoundError(f"Spark source not found: {path}")
    return path


def staging_out_dir(tmp_path: Path, case: SparkMigrateCase) -> Path:
    stem = Path(case.rel_path).stem
    return tmp_path / case.category / stem


def flink_reference_dir(case: SparkMigrateCase) -> Path:
    """Resolve ``references/flink/valid/{category}/{stem}`` for a migrate case."""
    from flink_skill_common.flink_sql_compare import reference_pipeline_dir

    return reference_pipeline_dir(case)


def run_and_assert_cli(case: SparkMigrateCase) -> Path:
    source = spark_source_path(case)
    statements_dir = statements_dir_for(source)
    if statements_dir.exists():
        shutil.rmtree(statements_dir)

    out_dir = staging_out_dir(output_path, case)

    result = runner.invoke(
        app,
        [
            "--table",
            case.target_table,
            "--file",
            str(source),
            "--out-dir",
            str(out_dir),
        ],
    )
    print(result.output)
    assert result.exit_code == 0, result.output
    return out_dir
