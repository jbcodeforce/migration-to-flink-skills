# KSQL Tutorial queries

This folder includes the [ksql queries from Confluent tutorials](https://developer.confluent.io/tutorials/).

## Routing
Deciding where to send events, splitting, merging, and filtering streams.

Folder sources/routing:

* [Filtering- Splitting](https://developer.confluent.io/confluent-tutorials/splitting/ksql/) `acting_events` to acting_events_fantasy, acting_events_other -> file `ksql/sources/routing/splitting.ksql`
* [Merge multiple streams](https://developer.confluent.io/confluent-tutorials/merging/ksql/) into one `all_songs` -> file `ksql/sources/routing/merge.ksql`
* [Filtering streams](https://developer.confluent.io/confluent-tutorials/filtering/ksql/) from `all_publications` -> file `ksql/sources/routing/filtering.ksql`
* [Deduplication](https://developer.confluent.io/confluent-tutorials/deduplication-windowed/ksql/) clicks events within a time window -> file `ksql/sources/routing/deduplicate.ksql`

## Transformation

* [Rekeying](https://developer.confluent.io/confluent-tutorials/rekeying/ksql/) to `ksql/sources/transformations/rekeying.ksql`
* [Convert serialization](https://developer.confluent.io/confluent-tutorials/serialization/ksql/) to `ksql/sources/transformations/conver_serdes.ksql`
* [colum difference](https://developer.confluent.io/confluent-tutorials/column-difference/ksql/) to  `ksql/sources/transformations/col_diff.ksql`
* [Geographic distance](https://developer.confluent.io/confluent-tutorials/geo-distance/ksql/) to `ksql/sources/transformations/geo_diff.ksql`
* [Scalar function](https://developer.confluent.io/confluent-tutorials/transforming/ksql/) to `ksql/sources/transformations/scalar_xform.ksql`
* [Flatten nested struct](https://developer.confluent.io/confluent-tutorials/flatten-nested-data/ksql/) to `ksql/sources/transformations/flatten_nested.ksql`
* [concat](https://developer.confluent.io/confluent-tutorials/concatenation/ksql/) to `ksql/sources/transformations/concat.ksql`
* [Mask data](https://developer.confluent.io/confluent-tutorials/masking-data/ksql/)

## Aggregations

Under sources/aggregations folder 

* [Count messages](https://developer.confluent.io/confluent-tutorials/count-messages/ksql/) to `ksql/sources/aggregations/count_pageviews.ksql`
* [Counting by key](https://developer.confluent.io/confluent-tutorials/aggregating-count/ksql/) to `ksql/sources/aggregations/aggregating-count.ksql`
* [count over](https://developer.confluent.io/confluent-tutorials/aggregating-sum/ksql/) to `ksql/sources/aggregations/aggregating-sum.ksql`
* [Min-max](https://developer.confluent.io/confluent-tutorials/aggregating-minmax/ksql/) to `ksql/sources/aggregations/aggregating-minmax.ksql`

## Windowing

* [tumbling](https://developer.confluent.io/confluent-tutorials/tumbling-windows/ksql/) to `ksql/sources/windows/tumbling.ksql`
* [Hoping](https://developer.confluent.io/confluent-tutorials/hopping-windows/ksql/) to `ksql/sources/windows/hoping.ksql`
* [Session](https://developer.confluent.io/confluent-tutorials/session-windows/ksql/) to `ksql/sources/windows/session.ksql`
* [Time zone conversion](https://developer.confluent.io/confluent-tutorials/convert-timestamp-timezone/ksql/)  to `ksql/sources/windows/time-tz.ksql`
* [Time concept](https://developer.confluent.io/confluent-tutorials/time-concepts/ksql/) to `ksql/sources/windows/evt-time.ksql`

By default, time-based aggregations in ksqlDB (tumbling windows, hopping windows, etc.) operate on the timestamp in the record metadata, which could be either 'CreateTime' (the producer system time) or 'LogAppendTime' (the broker system time), depending on the message.timestamp.type topic configuration value. 'CreateTime' may help with event-time semantics, but in some use cases, the desired event time is a timestamp embedded inside the record payload itself.

## Joins 

* [Join stream to stream](https://developer.confluent.io/confluent-tutorials/joining-stream-stream/ksql/) join two event streams on a common key in order to create a new enriched event stream. File in `ksql/sources/joins/stream_stream.ksql`
* [Join stream with table](https://developer.confluent.io/confluent-tutorials/joining-stream-table/ksql/) to `ksql/sources/joins/stream_table.ksql`
* [Join table with table](https://developer.confluent.io/confluent-tutorials/joining-table-table/ksql/) to `ksql/sources/joins/table_table.ksql`
* [Join multi-ways](https://developer.confluent.io/confluent-tutorials/multi-joins/ksql/) to `ksql/sources/joins/multi-joins.ksql`

## Miscellaneous
* [Deserialization error](https://developer.confluent.io/confluent-tutorials/deserialization-errors/ksql/) to `ksql/sources/misc/des-err.ksql`
* [Heterogeneous JSon](https://developer.confluent.io/confluent-tutorials/ksql-heterogeneous-json/ksql/) to `ksql/sources/misc/json.ksql`.

## Flink References

The flink_ref folder includes the matching references.

## Dry run some migrations

Use the `run_migration.sh`

* Basic usage
    ```sh
    ./run_migration.sh all_songs sources/routing/merge.ksql
    ```

* With custom staging directory
    ```sh
    STAGING=/custom/path ./run_migration.sh pageviews_count sources/aggregations/count_pageviews.ksql
    ```

* Show help
    ```sh
    ./run_migration.sh --help
    ```

### Migrations Examples

* Joins
```sh
STAGING=./staging ./run_migration.sh shipped_orders sources/joins/stream_stream.ksql
```