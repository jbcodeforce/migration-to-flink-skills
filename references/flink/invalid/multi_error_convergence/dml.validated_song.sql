INSERT INTO validated_songs
SELECT
  artist,
  title2,
  `$rowtime` as ts
FROM raw_classical_songs
WHERE title NOT NULL;
