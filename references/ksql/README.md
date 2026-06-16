# KSQL Tutorial queries

This folder includes the [ksql queries from Confluent tutorials](https://developer.confluent.io/tutorials/).

## Routing
Deciding where to send events, splitting, merging, and filtering streams.

Folder sources/routing:

* [Filtering- Splitting](https://developer.confluent.io/confluent-tutorials/splitting/ksql/) `acting_events` to acting_events_fantasy, acting_events_other -> file `splitting.ksql`
* [Merge multiple streams](https://developer.confluent.io/confluent-tutorials/merging/ksql/) into one `all_songs` -> file `merge.ksql`
* [Filtering streams](https://developer.confluent.io/confluent-tutorials/filtering/ksql/) from `all_publications` -> file `filtering.ksql`
* [Deduplication](https://developer.confluent.io/confluent-tutorials/deduplication-windowed/ksql/) clicks events within a time window -> file `deduplicate.ksql`


## Aggregations

Under sources/aggregations folder 

* [Count messages](https://developer.confluent.io/confluent-tutorials/count-messages/ksql/) from pageviews -> file `count_pageviews.ksql`

## Joins 

* [Join stream to stream](https://developer.confluent.io/confluent-tutorials/joining-stream-stream/ksql/) join two event streams on a common key in order to create a new enriched event stream.


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