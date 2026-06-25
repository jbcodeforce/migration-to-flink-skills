# Worked examples (ksql_tutorial golden pairs)

## merge.ksql → dim_all_songs

KSQL: `ksql_tutorial/sources/routing/merge.ksql`

- Defines `rock_songs`, `classical_songs`, `all_songs` streams
- Merges with `INSERT INTO all_songs`

Flink golden: `flink_ref/dimensions/songs/all_song/sql-scripts/`

- DDL: `dim_all_songs` with PK on `artist`
- DML: `UNION ALL` from `src_classical_songs` and `src_rock_songs`

## stream_stream.ksql → shipped_orders

KSQL: `ksql_tutorial/sources/joins/stream_stream.ksql`

- `orders` and `shipments` streams with interval join `WITHIN 7 DAYS`

Flink golden: `flink_ref/joins/shipped_orders/sql-scripts/`

- DDL: `shipped_orders` keyed on `order_id`
- DML: interval join using Flink streaming join semantics

## splitting.ksql → dim_acting_events_drama

KSQL: `ksql_tutorial/sources/routing/splitting.ksql`

- Splits `acting_events` by genre into drama, fantasy, other streams

Flink golden (drama branch): `flink_ref/dimensions/acting_events/acting_events_drama/sql-scripts/`

- DDL: `dim_acting_events_drama` with PK on `name`
- DML: `INSERT INTO` filtered `WHERE genre = 'drama'`

## Sources without flink_ref goldens

These ksql tutorial files have no matching `flink_ref` output yet:

- `sources/routing/filtering.ksql`
- `sources/routing/deduplicate.ksql`
- `sources/aggregations/count_pageviews.ksql`

Use for manual migration experiments only.

## KMA-CHAT.sql → kma_chat (GROUP BY + LATEST_BY_OFFSET)

KSQL: `CREATE TABLE KES_ICE_CHAT_DEAL_TB ... AS SELECT` with mixed bare columns and `LATEST_BY_OFFSET(...)` aggregates, grouped by `msg_from_id`, `msg_to_id`, `msg_epoch`, `msg_incoming`.

Flink DML pattern (mandatory when ksql has `GROUP BY` + `LATEST_BY_OFFSET`):

```sql
INSERT INTO kma_chat
WITH deduplicated AS (
    SELECT
        `msg_type`,
        `msg_epoch`,
        `msg_timestamp`,
        `msg_from_id`,
        `msg_incoming`,
        `msg_to_id`,
        `msg_body`,
        `username`,
        `company`,
        `firstname`,
        `lastname`
    FROM (
        SELECT
            `msg_type`,
            `msg_epoch`,
            `msg_timestamp`,
            `msg_from_id`,
            `msg_incoming`,
            `msg_to_id`,
            `msg_body`,
            `username`,
            `company`,
            `firstname`,
            `lastname`,
            ROW_NUMBER() OVER (
                PARTITION BY `msg_from_id`, `msg_to_id`, `msg_epoch`, `msg_incoming`
                ORDER BY $rowtime DESC
            ) AS rn
        FROM kma_chat_st
    )
    WHERE rn = 1
)
SELECT * FROM deduplicated
GROUP BY `msg_from_id`, `msg_to_id`, `msg_epoch`, `msg_incoming`;
```

Notes:

- `PARTITION BY` matches ksql `GROUP BY` exactly.
- Outer `GROUP BY` must not be omitted.
- Sink DDL uses composite `PRIMARY KEY` on the same group-by columns with `'changelog.mode' = 'upsert'` for compact topics.
