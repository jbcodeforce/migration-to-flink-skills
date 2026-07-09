CREATE TABLE IF NOT EXISTS src_acting_events (
    name STRING,
    title STRING,
    genre STRING,
    PRIMARY KEY (name) NOT ENFORCED
) DISTRIBUTED BY HASH(name) INTO 1 BUCKETS WITH (
    'changelog.mode' = 'append'
) 
AS SELECT
    name, 
    title,
    genre
(VALUES
    ('C.S. Lewis', 'The Silver Chair'),
    ('George R. R. Martin', 'A Song of Ice and Fire'),
    ('C.S. Lewis', 'Perelandra'),
    ('George R. R. Martin', 'Fire & Blood'),
    ('J. R. R. Tolkien', 'The Hobbit'),
    ('J. R. R. Tolkien', 'The Lord of the Rings'),
    ('George R. R. Martin', 'A Dream of Spring'),
    ('J. R. R. Tolkien', 'The Fellowship of the Ring'),
    ('George R. R. Martin', 'The Ice Dragon')
) as seed (name, title, genre);