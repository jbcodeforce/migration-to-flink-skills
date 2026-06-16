# Fixture paths

Relative to `Code/flink_project_demos/ksql_tutorial/`.

## Golden pairs (CI)

| Name | KSQL | Flink DDL | Flink DML |
|------|------|-----------|-----------|
| merge | `sources/routing/merge.ksql` | `flink_ref/dimensions/songs/all_song/sql-scripts/ddl.dim_all_song.sql` | `dml.dim_all_song.sql` |
| shipped_orders | `sources/joins/stream_stream.ksql` | `flink_ref/joins/shipped_orders/sql-scripts/ddl.shipped_orders.sql` | `dml.shipped_orders.sql` |
| acting_events_drama | `sources/routing/splitting.ksql` | `flink_ref/dimensions/acting_events/acting_events_drama/sql-scripts/ddl.dim_acting_events_drama.sql` | `dml.dim_acting_events_drama.sql` |

## Manual only (no flink_ref golden)

- `sources/routing/filtering.ksql`
- `sources/routing/deduplicate.ksql`
- `sources/aggregations/count_pageviews.ksql`

## Supporting flink_ref pipelines

Source tables used by merge/splitting goldens:

- `flink_ref/sources/songs/rock/`
- `flink_ref/sources/songs/classical/`
- `flink_ref/sources/acting_events/acting_events/`
