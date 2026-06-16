INSERT INTO dim_acting_events_other
SELECT name, title, genre
FROM src_acting_events
WHERE genre <> 'drama' AND genre <> 'fantasy';