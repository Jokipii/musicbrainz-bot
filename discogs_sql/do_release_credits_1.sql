DROP TABLE IF EXISTS discogs_db_release_credits;

SELECT mb_release_link.gid AS release_gid, mb_release_link.url AS release_url, mb_artist_link.gid AS artist_gid, mb_artist_link.url AS artist_url, releases_extraartists.roles
INTO discogs_db_release_credits
FROM mb_release_link
JOIN releases_extraartists ON mb_release_link.id = releases_extraartists.release_id
JOIN artist ON releases_extraartists.artist_id = artist.id
JOIN mb_artist_link ON mb_artist_link.id = artist.id;

DELETE FROM discogs_db_release_credits WHERE release_gid IN
(SELECT gid FROM mb_release_link GROUP BY gid HAVING count(gid) > 1);

DELETE FROM discogs_db_release_credits WHERE artist_gid IN
(SELECT gid FROM mb_artist_link GROUP BY gid HAVING count(gid) > 1);
