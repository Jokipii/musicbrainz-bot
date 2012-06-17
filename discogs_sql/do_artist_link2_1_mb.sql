DROP TABLE IF EXISTS do_recordings;

SELECT DISTINCT do_release_link.url, do_release_link.name AS release_name, do_release_link.gid as release_gid, 
track_name.name AS track_name, recording.gid AS recording_gid, artist_name.name AS artist_name, artist.gid AS artist_gid 
INTO do_recordings
FROM do_release_link
JOIN release ON release.id = do_release_link.id 
JOIN medium ON medium.release = release.id
JOIN tracklist ON medium.tracklist = tracklist.id
JOIN track ON track.tracklist = tracklist.id
JOIN recording ON track.recording = recording.id
JOIN artist_credit ON recording.artist_credit = artist_credit.id
JOIN artist_credit_name ON artist_credit_name.artist_credit = artist_credit.id
JOIN artist ON artist_credit_name.artist = artist.id
JOIN artist_name ON artist_credit_name.name = artist_name.id
JOIN track_name ON recording.name = track_name.id
WHERE release.artist_credit = 1
AND artist_credit.artist_count = 1
AND artist.id NOT IN (SELECT id FROM do_artist_link);
