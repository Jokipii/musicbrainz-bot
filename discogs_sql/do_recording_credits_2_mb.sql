DROP TABLE IF EXISTS remix_temp;

SELECT t1.artist_gid, recording.gid, 'Release "'||t1.title||'" http://www.musicbrainz.org/release/'
||release.gid||' has Discogs link http://www.discogs.com/release/'||t1.id||'
There is track "'||track_name.name||'" that is same as recording http://www.musicbrainz.org/recording/'
||recording.gid||'
It is remixed by '||t1.name||' linked artist found on MusicBrainz http://www.musicbrainz.org/artist/'
||t1.artist_gid AS note
INTO remix_temp
FROM dblink(:dblink, '
    SELECT gid, id, title, position, track_title, name, artist_gid
    FROM remix_temp
') AS t1(gid uuid, id integer, title text, position text, track_title text, name text, artist_gid uuid)
JOIN release ON release.gid = t1.gid
JOIN medium ON medium.release = release.id
JOIN tracklist ON tracklist.id = medium.tracklist
JOIN track ON track.tracklist = tracklist.id
JOIN recording ON recording.id = track.recording
JOIN track_name ON track_name.id = recording.name
WHERE lower(track_name.name) = lower(t1.track_title);

WITH remove AS (
	SELECT remix_temp.artist_gid, remix_temp.gid
	FROM remix_temp
	JOIN artist ON artist.gid = remix_temp.artist_gid
	LEFT JOIN l_artist_recording ON l_artist_recording.entity0 = artist.id
	JOIN link ON link.id = l_artist_recording.link
	JOIN recording ON recording.id = l_artist_recording.entity1
	WHERE link.link_type = 153
	AND recording.gid = remix_temp.gid
)
DELETE
FROM remix_temp
USING remove
WHERE remix_temp.artist_gid = remove.artist_gid AND remix_temp.gid = remove.gid;

SELECT artist_gid, gid, (array_agg(note))[1]
INTO remix_temp2
FROM remix_temp
GROUP BY artist_gid, gid
ORDER BY (array_agg(note))[1];

DROP TABLE remix_temp;

ALTER TABLE remix_temp2 RENAME TO remix_temp;