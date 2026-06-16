INSERT INTO dim_acting_events_drama
SELECT name, title
FROM src_acting_events
WHERE genre = 'drama';