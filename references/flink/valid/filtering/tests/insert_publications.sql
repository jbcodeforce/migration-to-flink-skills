create table all_publications (
    book_id INT NOT NULL,
    author STRING,
    title STRING,
    PRIMARY KEY (book_id) NOT ENFORCED
) DISTRIBUTED BY HASH(book_id) INTO 1 BUCKETS WITH (
    'changelog.mode' = 'append',
    'kafka.retention.time' = '0',
    'kafka.producer.compression.type' = 'snappy',
    'scan.bounded.mode' = 'unbounded',
    'scan.startup.mode' = 'earliest-offset',
    'value.fields-include' = 'all'
) as select
    book_id,
    author,
    title
from VALUES(
    (1, 'C.S. Lewis', 'The Silver Chair'),
    (2, 'George R. R. Martin', 'A Song of Ice and Fire'),
    (3, 'C.S. Lewis', 'Perelandra'),
    (4, 'George R. R. Martin', 'Fire & Blood'),
    (5, 'J. R. R. Tolkien', 'The Hobbit'),
    (6, 'J. R. R. Tolkien', 'The Lord of the Rings'),
    (7, 'George R. R. Martin', 'A Dream of Spring'),
    (8, 'J. R. R. Tolkien', 'The Fellowship of the Ring'),
    (9, 'George R. R. Martin', 'The Ice Dragon')
) as seed (book_id, author, title);