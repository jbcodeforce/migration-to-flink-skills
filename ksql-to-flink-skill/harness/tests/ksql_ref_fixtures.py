"""Shared helpers for references/ksql tutorial migration integration tests."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
KSQL_SOURCES_ROOT = REPO_ROOT / "references" / "ksql" / "sources"


@dataclass(frozen=True)
class KsqlMigrateCase:
    """One ksqlDB tutorial source file and its Flink migration target table."""

    rel_path: str
    target_table: str
    category: str
    notes: str = ""


KSQL_MIGRATE_CASES: list[KsqlMigrateCase] = [
    # routing
    KsqlMigrateCase("routing/filtering.ksql", "george_martin", "routing"),
    KsqlMigrateCase("routing/merge.ksql", "dim_all_songs", "routing", "Flink name for ksql all_songs"),
    KsqlMigrateCase("routing/splitting.ksql", "dim_acting_events_drama", "routing", "Flink name for acting_events_drama"),
    KsqlMigrateCase("routing/deduplicate.ksql", "detected_clicks", "routing"),
    # joins
    KsqlMigrateCase("joins/stream_stream.ksql", "shipped_orders", "joins"),
    KsqlMigrateCase("joins/stream_table.ksql", "rated_movies", "joins"),
    KsqlMigrateCase("joins/table_table.ksql", "movies_enriched", "joins"),
    KsqlMigrateCase("joins/multi-joins.ksql", "orders_enriched", "joins"),
    # aggregations
    KsqlMigrateCase("aggregations/count_pageviews.ksql", "pageviews_count", "aggregations"),
    KsqlMigrateCase("aggregations/aggregating-count.ksql", "movie_ticket_sales", "aggregations"),
    KsqlMigrateCase("aggregations/aggregating-sum.ksql", "movie_ticket_sales", "aggregations"),
    KsqlMigrateCase("aggregations/aggregating-minmax.ksql", "movie_sales", "aggregations"),
    # windows
    KsqlMigrateCase("windows/tumbling.ksql", "ratings", "windows"),
    KsqlMigrateCase("windows/hoping.ksql", "average_temps", "windows"),
    KsqlMigrateCase("windows/session.ksql", "clicks", "windows"),
    KsqlMigrateCase("windows/evt-time.ksql", "temperature_event_time", "windows"),
    KsqlMigrateCase("windows/time-tz.ksql", "temperature_readings_raw", "windows"),
    # transformations
    KsqlMigrateCase("transformations/col_diff.ksql", "customer_purchases", "transformations"),
    KsqlMigrateCase("transformations/concat.ksql", "activity_summary", "transformations"),
    KsqlMigrateCase("transformations/convert_serdes.ksql", "movies_proto", "transformations"),
    KsqlMigrateCase("transformations/flatten_nested.ksql", "flattened_orders", "transformations"),
    KsqlMigrateCase("transformations/geo_diff.ksql", "insurance_event_with_repair_info", "transformations"),
    KsqlMigrateCase("transformations/maks_data.ksql", "purchases_pii_obfuscated", "transformations"),
    KsqlMigrateCase("transformations/rekeying.ksql", "movies_by_title", "transformations"),
    # misc
    KsqlMigrateCase("misc/des-err.ksql", "sensors_raw", "misc"),
    KsqlMigrateCase("misc/json.ksql", "data_stream", "misc"),
]

KSQL_EXCLUDED_SOURCES: list[tuple[str, str]] = [
    ("transformations/scalar_xform.ksql", "SELECT-only; no CREATE STREAM/TABLE"),
    ("transformations/insert_movies.ksql", "INSERT seed data only"),
    ("transformations/insert_purchases.ksql", "INSERT seed data only"),
]


def ksql_source_path(case: KsqlMigrateCase) -> Path:
    path = KSQL_SOURCES_ROOT / case.rel_path
    if not path.is_file():
        raise FileNotFoundError(f"KSQL source not found: {path}")
    return path


def staging_out_dir(tmp_path: Path, case: KsqlMigrateCase) -> Path:
    stem = Path(case.rel_path).stem
    return tmp_path / case.category / stem
