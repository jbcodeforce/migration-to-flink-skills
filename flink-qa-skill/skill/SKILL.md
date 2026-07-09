---
name: flink-qa
description: >-
  Help answering Confluent Flink 101 type of questions 
---
You are an expert in Confluent Cloud for Flink SQL helping user to troubleshoot common issues. Here is a list of frequently asked questions.

## QUESTIONS

<question>How to identify watermark issues?</question>
<answer>Watermarks are special markers periodically injected by Flink source operator, to indicate event-time progress in each stream. They track how time advances and help handle out-of-order records.

Confluent Cloud for Apache Flink provides a default watermark strategy based on the `$rowtime` column for all tables, whether created automatically from a Kafka topic or using a CREATE TABLE statement. Watermark is computed per Kafka partition, with at least a minimum of 250 records per partition.
Events arriving after watermarks are considered late and are typically discarded.
The default strategy is designed for large-scale production workloads, requiring a significant volume of data (around 250 events per partition) before advancing the watermark and emitting results.
Watermarks are generated independently for each stream and partition. When two partitions are combined, the resulting watermark will be the oldest of the two (min value), reflecting the point at which the system has complete information. If one partition stops receiving new events, the watermark for that partition will not progress. To ensure that processing continues over time, an idle timeout configuration can be implemented.

In Confluent Cloud, a progressive idleness detection feature is used by default, which sets the idle timeout to 15 seconds, increasing up to 5 minutes. The rational is to avoid waiting too long to see watermarks progressing, and do not set partition idle too quickly neither. The default watermark applies to $rowtime. The following setting disables the progressive timeout.
    ```sql
    SET 'sql.tables.scan.idle-timeout' = '1s';
    ```

Idle Partition Timeout must always be less than or equal to Max Allowed Drift used for Watermark Alignment.

If a partition receives no events, no watermark may be generated for it; the combined watermark may not advance, and windows may fail to produce results. To avoid this, balance Kafka partitions so none stay empty or idle for long, or configure watermarking with idleness detection.
Using a custom timestamp attribute for watermark strategy, events may be heavilty out-of-order within a partition.
### Common issues due to watermarks

* **Records may not appear in the output table or topic**. When testing with only a few events, this fails to meet the initial "safety margin" of 250 events per partition. This causes the system to apply a massive 7-day default margin, which stalls the watermark indefinitely and prevents time windows from ever closing and producing a result.
* **Stalled Joins with Idle Sources**: When joining two streams, if one stream is idle or has very old data, its watermark remains far in the past. The join operator's watermark becomes the minimum of the two, effectively stalling the entire query and preventing any new join results from being produced, even when one stream is active.  if all their input partitions are marked Idle, the Subtask becomes Idle and emits an Idle signal downstream.
* **Operators Not Propagating Watermarks**: Some operators remove the metadata marker identifying the event-time attribute, making Watermarks unusable downstream. The operators dropping the event-time attribute marker (not a complete list):
    * Regular JOINs - both INNER and OUTER equi-JOIN
    * Window Time Value Function aggregations not including window_time in the GROUP BY - window_time is propagated as time attribute, but window_start and window_end are not.
    * Top-N Queries (Ranking) - Queries using ROW_NUMBER() and filtering by the top-N results
    * Global Distinct -  DISTINCT across the entire history 
    * Set operations  - UNION, INTERSECT, EXCEPT. Except UNION ALL which does not remove duplicates and does propagate the event-time attribute.

    **If you need a time-based operation downstream of operators that drop Watermarks, emit the result to Kafka and read it back with a different statement.**

* **Operators Delaying Watermarks:** Certain operators (Interval Joins, MATCH_RECOGNIZE) may add significant delay to Watermarks. The following interval join will deplay the watermark by 60 minutes, because it will match transactions that may be 6 hours older than matching stock
    ```sql
    SELECT t.amount, t.order_type, s.name, s.opening_value 
    FROM transactions t, stocks s
    WHERE t.stockid = s.id AND t.ts BETWEEN s.ts - INTERVAL '6' HOURS AND s.ts
    ```

    Similarly, a MATCH_RECOGNIZE with a clause WITHIN INTERVAL '15’ MINUTE will delay watermarks by 15 minutes.

* **Losing the Last Message:** In a sparse stream of events, the very last event is correctly placed in its time window but remains buffered. Because no new event ever arrives to advance the watermark past the end of that final window, the window never closes, and the result for the last message is never produced, making it seem like Flink lost data. One way to avoid that is to send heartbeat message to the input topic.
</answer>