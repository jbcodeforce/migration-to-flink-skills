INSERT INTO dim_all_songs
SELECT
  artist,
  title,
  'classical' as genre
FROM src_classical_songs
UNION ALL
SELECT
  artist,
  title,
  'rock' as genre
FROM src_rock_songs
