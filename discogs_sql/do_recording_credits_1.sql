DROP TABLE IF EXISTS remix_temp;

WITH a AS (
	SELECT mb_release_link.gid, release.id, release.title, track.position, track.title AS track_title, tracks_extraartists.artist_id
	FROM mb_release_link
	JOIN release ON release.id = mb_release_link.id
	JOIN track ON track.release_id = release.id
	JOIN tracks_extraartists ON tracks_extraartists.track_id = track.id
	WHERE role_name = 'Remix'
	AND role_details ISNULL
	AND anv = ''
	AND artist_id <> 0
)	
SELECT a.gid, a.id, a.title, a.position, a.track_title, artist.name, mb_artist_link.gid AS artist_gid
INTO remix_temp
FROM a
JOIN artist ON artist.id = a.artist_id
JOIN mb_artist_link ON mb_artist_link.id = artist.id
WHERE lower(mb_artist_link.name) = lower(substring(artist.name FROM '(.+?)(?: \(\d+\))?$'))