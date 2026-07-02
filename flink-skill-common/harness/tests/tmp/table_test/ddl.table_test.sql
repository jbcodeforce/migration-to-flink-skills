create table table_test (
        id int,
        name string,
        product string,
        product_name string,
        PRIMARY KEY (id) NOT ENFORCED
    ) DISTRIBUTED BY HASH(id) INTO 6 BUCKETS WITH (
        'changelog.mode' = 'upsert',
        'key.format' = 'avro-registry',
        'value.format' = 'avro-registry',
    );