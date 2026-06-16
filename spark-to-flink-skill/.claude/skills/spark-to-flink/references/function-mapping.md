# Spark to Flink function mapping

| Spark / dbt | Flink |
|-------------|-------|
| `surrogate_key(a, b, ...)` | `MD5(CONCAT_WS(',', a, b, ...))` |
| `current_timestamp()` | `$rowtime` |
| `CURRENT_DATE()` | `CURRENT_DATE` |
| `DATEDIFF(end, start)` | `TIMESTAMPDIFF(DAY, CAST(start AS TIMESTAMP_LTZ(3)), end)` |
| `date_trunc('month', ts)` | `DATE_FORMAT(ts, 'yyyy-MM')` (context-dependent) |
| `split_part(s, delim, n)` | `REGEXP_EXTRACT(s, pattern, n)` |
| `PERCENTILE_APPROX(col, p)` | `PERCENTILE(col, p)` or approximate via window (verify CC support) |
| `EXPLODE(array)` | `CROSS JOIN UNNEST(array) AS t(element)` |
| `LENGTH(s)` | `CHARACTER_LENGTH(s)` |
| `LEFT ANTI JOIN` | `LEFT JOIN` + `WHERE right.key IS NULL` |

## Windowing

| Spark batch pattern | Flink streaming |
|---------------------|-----------------|
| Time bucket aggregation | `TABLE(TUMBLE(TABLE src, DESCRIPTOR($rowtime), INTERVAL '1' HOUR))` |
| Session gaps | `TABLE(SESSION(TABLE src, DESCRIPTOR($rowtime), INTERVAL '30' MINUTE))` |
| Sliding window | `TABLE(HOP(TABLE src, DESCRIPTOR($rowtime), INTERVAL '5' MINUTE, INTERVAL '1' HOUR))` |

## Deduplication

Spark `ROW_NUMBER() ... WHERE row_num = 1` maps directly. Use `$rowtime DESC` for stream ordering instead of `registration_date DESC` when source is Kafka CDC.
