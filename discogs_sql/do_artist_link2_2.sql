DROP TABLE IF EXISTS discogs_db_artist_link2;

CREATE TABLE discogs_db_artist_link2
(
  artist_gid uuid,
  url text,
  releases text
);

WITH mb_recordings2 AS (
    WITH mb_recordings AS (
        SELECT DISTINCT decodeURL(t1.url, 'http://www.discogs.com/release/') AS release_id, t1.*  
        FROM dblink(:dblink, 'SELECT url, release_name, release_gid, track_name, recording_gid, artist_name, artist_gid FROM do_recordings')
        AS t1(url text, release_name text, release_gid uuid, track_name text, recording_gid uuid, artist_name text, artist_gid uuid)
    )
    SELECT DISTINCT mb_recordings.artist_name, artist_gid, 'Recording: ' || track_name 
    || ' by ' || mb_recordings.artist_name || ' in release "' || release_name 
    || '": http://musicbrainz.org/release/' || release_gid || 
    ' is linked to Discogs release ' || url || ' Track credits points to same artist' AS note
    FROM mb_recordings
    JOIN track ON mb_recordings.release_id = track.release_id::text
    JOIN tracks_artists ON tracks_artists.track_id = track.id
    WHERE mb_recordings.track_name = track.title
    AND mb_recordings.artist_name = tracks_artists.artist_name
), mb_recordings3 AS (
    SELECT artist_gid, 'http://www.discogs.com/artist/' || url (artist_name) AS url, 'Artist "' 
    || artist_name || '" with exact match on name found on Discogs.
    ' || string_agg(note, ',
    ') AS releases
    FROM mb_recordings2
    GROUP BY artist_gid, artist_name
)
INSERT INTO discogs_db_artist_link2 (artist_gid, url, releases)
SELECT artist_gid, url, releases FROM mb_recordings3
EXCEPT
SELECT artist_gid, url, releases FROM discogs_db_artist_link
ORDER BY url;
