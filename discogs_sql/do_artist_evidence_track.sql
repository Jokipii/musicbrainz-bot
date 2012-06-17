DROP TABLE IF EXISTS discogs_db_artist_evidence_track;

SELECT t1.artist_gid AS gid, url(tracks_artists.artist_name), 
	'Track "'||t1.name||'" artist on linked Discogs release http://www.musicbrainz.org/release/'||t1.gid||' pointing to same artist.' AS note,
	1 AS score
INTO discogs_db_artist_evidence_track
FROM dblink(:dblink, '
	SELECT artist.gid, artist_name.name AS artist_name, release.gid, track_name.name
	FROM recording
	JOIN artist_credit ON artist_credit.id = recording.artist_credit
	JOIN artist_credit_name ON artist_credit_name.artist_credit = artist_credit.id
	JOIN artist ON artist.id = artist_credit_name.artist
	JOIN artist_name ON artist_name.id = artist.name
	JOIN track ON track.recording = recording.id
	JOIN tracklist ON tracklist.id = track.tracklist
	JOIN medium ON medium.tracklist = tracklist.id
	JOIN release ON release.id = medium.release
	JOIN track_name ON track_name.id = recording.name
	WHERE artist.id NOT IN (SELECT id FROM do_artist_link)
	AND release.id IN (SELECT id FROM do_release_link)
') AS t1(artist_gid uuid, artist_name text, gid uuid, name text)
JOIN mb_release_link ON mb_release_link.gid = t1.gid
JOIN track ON track.release_id = mb_release_link.id AND lower(track.title) = lower(t1.name)
JOIN tracks_artists ON tracks_artists.track_id = track.id
WHERE lower(substring(tracks_artists.artist_name FROM '(.+?)(?: \(\d+\))?$')) = lower(t1.artist_name)
