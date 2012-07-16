DROP TABLE IF EXISTS discogs_db_artist_evidence_release_credits;

WITH credits AS (
	SELECT t1.artist_gid AS gid, url(releases_extraartists.artist_name), 'Artist credited ('||roles.credit
		||') in http:\\musicbrainz.org\release\'||t1.release_gid
		||' and in linked Discogs release '||mb_release_link.url AS note
	FROM dblink(:dblink, '
		SELECT artist.gid AS artist_gid, release.gid AS release_gid, link.link_type, lower(artist_name.name) AS artist_name
		FROM l_artist_release
		JOIN artist ON (artist.id = l_artist_release.entity0 AND NOT artist.gid IN (SELECT gid FROM do_artist_link))
		JOIN artist_name ON artist_name.id = artist.name
		JOIN release ON (release.id = l_artist_release.entity1 AND release.gid IN (SELECT gid FROM do_release_link))
		JOIN link ON link.id = l_artist_release.link
	') AS t1(artist_gid uuid, release_gid uuid, link_type integer, artist_name text)
	JOIN mb_release_link ON mb_release_link.gid = t1.release_gid
	JOIN releases_extraartists ON (releases_extraartists.release_id = mb_release_link.id AND 
		lower(substring(releases_extraartists.artist_name FROM '(.+?)(?: \(\d+\))?$')) = t1.artist_name)
	JOIN roles ON (roles.credit = releases_extraartists.role_name AND roles.release_link_type = t1.link_type)
)
SELECT gid, url, array_agg(note) AS note, count(note) AS sum
INTO discogs_db_artist_evidence_release_credits
FROM credits
GROUP BY gid, url
ORDER BY count(note) DESC;
