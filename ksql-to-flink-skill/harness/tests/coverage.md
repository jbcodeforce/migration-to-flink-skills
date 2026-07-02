# Coverage for testing ksql to flink

## Integration tests

**26** parametrized cases in [`it/test_ksql_migrate_it.py`](it/test_ksql_migrate_it.py), one per migratable tutorial `.ksql` file. Each runs `ksql-flink-migrate` end-to-end (LLM translation + offline validation + CC Flink deploy).

**Prerequisites:** `SL_LLM_*` env (reachable LLM) and CC Flink deploy env (same as `flink-skill-common` IT). Tests skip automatically when either is missing.

```bash
cd ksql-to-flink-skill/harness
uv run pytest tests/it/test_ksql_migrate_it.py -m integration -v
# single case:
uv run pytest tests/it/test_ksql_migrate_it.py -k "routing/filtering" -vs
```

Fixture manifest: [`ksql_ref_fixtures.py`](ksql_ref_fixtures.py).

| Category | Source file | `--table` | Test id |
|----------|-------------|-----------|---------|
| routing | `routing/filtering.ksql` | `george_martin` | `routing/filtering.ksql` |
| routing | `routing/merge.ksql` | `dim_all_songs` | `routing/merge.ksql` |
| routing | `routing/splitting.ksql` | `dim_acting_events_drama` | `routing/splitting.ksql` |
| routing | `routing/deduplicate.ksql` | `detected_clicks` | `routing/deduplicate.ksql` |
| joins | `joins/stream_stream.ksql` | `shipped_orders` | `joins/stream_stream.ksql` |
| joins | `joins/stream_table.ksql` | `rated_movies` | `joins/stream_table.ksql` |
| joins | `joins/table_table.ksql` | `movies_enriched` | `joins/table_table.ksql` |
| joins | `joins/multi-joins.ksql` | `orders_enriched` | `joins/multi-joins.ksql` |
| aggregations | `aggregations/count_pageviews.ksql` | `pageviews_count` | `aggregations/count_pageviews.ksql` |
| aggregations | `aggregations/aggregating-count.ksql` | `movie_ticket_sales` | `aggregations/aggregating-count.ksql` |
| aggregations | `aggregations/aggregating-sum.ksql` | `movie_ticket_sales` | `aggregations/aggregating-sum.ksql` |
| aggregations | `aggregations/aggregating-minmax.ksql` | `movie_sales` | `aggregations/aggregating-minmax.ksql` |
| windows | `windows/tumbling.ksql` | `ratings` | `windows/tumbling.ksql` |
| windows | `windows/hoping.ksql` | `average_temps` | `windows/hoping.ksql` |
| windows | `windows/session.ksql` | `clicks` | `windows/session.ksql` |
| windows | `windows/evt-time.ksql` | `temperature_event_time` | `windows/evt-time.ksql` |
| windows | `windows/time-tz.ksql` | `temperature_readings_raw` | `windows/time-tz.ksql` |
| transformations | `transformations/col_diff.ksql` | `customer_purchases` | `transformations/col_diff.ksql` |
| transformations | `transformations/concat.ksql` | `activity_summary` | `transformations/concat.ksql` |
| transformations | `transformations/convert_serdes.ksql` | `movies_proto` | `transformations/convert_serdes.ksql` |
| transformations | `transformations/flatten_nested.ksql` | `flattened_orders` | `transformations/flatten_nested.ksql` |
| transformations | `transformations/geo_diff.ksql` | `insurance_event_with_repair_info` | `transformations/geo_diff.ksql` |
| transformations | `transformations/maks_data.ksql` | `purchases_pii_obfuscated` | `transformations/maks_data.ksql` |
| transformations | `transformations/rekeying.ksql` | `movies_by_title` | `transformations/rekeying.ksql` |
| misc | `misc/des-err.ksql` | `sensors_raw` | `misc/des-err.ksql` |
| misc | `misc/json.ksql` | `data_stream` | `misc/json.ksql` |

### Excluded from IT (no CREATE STREAM/TABLE)

| Source file | Reason |
|-------------|--------|
| `transformations/scalar_xform.ksql` | SELECT-only |
| `transformations/insert_movies.ksql` | INSERT seed data only |
| `transformations/insert_purchases.ksql` | INSERT seed data only |

`.sql` seed files (`insert_pageviews.sql`, etc.) are out of scope for migration IT.

---

## Tutorial ksql sources

The current sources for all the tutorial ksql statements are in references/ksql/sources

```
├── aggregations
│   ├── aggregating-count.ksql
│   ├── aggregating-minmax.ksql
│   ├── aggregating-sum.ksql
│   ├── count_pageviews.ksql
│   └── insert_pageviews.sql
├── joins
│   ├── multi-joins.ksql
│   ├── stream_stream.ksql
│   ├── stream_table.ksql
│   └── table_table.ksql
├── misc
│   ├── des-err.ksql
│   └── json.ksql
├── routing
│   ├── deduplicate.ksql
│   ├── filtering.ksql
│   ├── insert_acting_events.sql
│   ├── insert_clicks.sql
│   ├── insert_songs.sql
│   ├── merge.ksql
│   └── splitting.ksql
├── transformations
│   ├── col_diff.ksql
│   ├── concat.ksql
│   ├── convert_serdes.ksql
│   ├── flatten_nested.ksql
│   ├── geo_diff.ksql
│   ├── insert_movies.ksql
│   ├── insert_purchases.ksql
│   ├── maks_data.ksql
│   ├── rekeying.ksql
│   └── scalar_xform.ksql
└── windows
    ├── evt-time.ksql
    ├── hoping.ksql
    ├── session.ksql
    ├── time-tz.ksql
    └── tumbling.ksql
```
