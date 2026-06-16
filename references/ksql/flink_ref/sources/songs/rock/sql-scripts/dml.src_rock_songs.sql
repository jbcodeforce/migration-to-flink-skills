INSERT INTO src_rock_songs
SELECT 
  artist, 
  title
 FROM(
    SELECT
     *,
     ROW_NUMBER() OVER (PARTITION BY artist ORDER BY $rowtime DESC) AS rownum
    FROM raw_rock_songs
 ) WHERE rownum = 1
