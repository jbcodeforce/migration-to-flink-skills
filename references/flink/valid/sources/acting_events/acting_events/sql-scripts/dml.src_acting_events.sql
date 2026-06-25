INSERT INTO src_acting_events
SELECT 
  name, 
  title,
  genre
 FROM(
    SELECT
     *,
     ROW_NUMBER() OVER (PARTITION BY name ORDER BY $rowtime DESC) AS rownum
    FROM raw_acting_events
 ) WHERE rownum = 1
