DROP TABLE IF EXISTS release_track_count;

SELECT release_id, COUNT(id)
INTO release_track_count
FROM track
WHERE track.position <> ''
GROUP BY release_id;
