CREATE TABLE IF NOT EXISTS all_publications (
  bookid BIGINT,
  author STRING,
  title STRING,
  PRIMARY KEY (bookid) NOT ENFORCED
) DISTRIBUTED BY HASH (bookid) INTO 6 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'kafka.retention.time' = '0',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);