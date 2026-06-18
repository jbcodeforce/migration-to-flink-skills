from __future__ import annotations

from flink_skill_common.fixtures import GoldenPair
from flink_skill_common.fixtures import assert_fixtures_exist as _assert_fixtures_exist

from ksql_flink_skill.config import flink_ref_root, ksql_sources_root


def ksql_golden_pairs() -> list[GoldenPair]:
    sources = ksql_sources_root()
    ref = flink_ref_root()

    return [
        GoldenPair(
            name="merge",
            source_file=sources / "routing/merge.ksql",
            flink_ddl=ref / "dimensions/songs/all_song/sql-scripts/ddl.dim_all_song.sql",
            flink_dml=ref / "dimensions/songs/all_song/sql-scripts/dml.dim_all_song.sql",
            table_name="dim_all_songs",
        ),
        GoldenPair(
            name="shipped_orders",
            source_file=sources / "joins/stream_stream.ksql",
            flink_ddl=ref / "joins/shipped_orders/sql-scripts/ddl.shipped_orders.sql",
            flink_dml=ref / "joins/shipped_orders/sql-scripts/dml.shipped_orders.sql",
            table_name="shipped_orders",
        ),
        GoldenPair(
            name="acting_events_drama",
            source_file=sources / "routing/splitting.ksql",
            flink_ddl=ref / "dimensions/acting_events/acting_events_drama/sql-scripts/ddl.dim_acting_events_drama.sql",
            flink_dml=ref / "dimensions/acting_events/acting_events_drama/sql-scripts/dml.dim_acting_events_drama.sql",
            table_name="dim_acting_events_drama",
        ),
    ]


def assert_ksql_fixtures_exist() -> None:
    _assert_fixtures_exist(ksql_golden_pairs())


assert_fixtures_exist = assert_ksql_fixtures_exist
