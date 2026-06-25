INSERT INTO dim_acting_events_fantasy
SELECT name, title
FROM src_acting_events
WHERE genre = 'fantasy';