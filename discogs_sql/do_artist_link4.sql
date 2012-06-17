DROP TABLE IF EXISTS discogs_db_artist_link4;

WITH u AS (
	SELECT * FROM dblink(:dblink, '
		SELECT s_artist.gid, s_artist.name, url.url
		FROM s_artist
		JOIN l_artist_url ON l_artist_url.entity0 = s_artist.id
		JOIN url ON url.id = l_artist_url.entity1
		AND s_artist.id NOT IN (SELECT id FROM do_artist_link)
	') AS t1(gid uuid, name text, url text)
)
SELECT u.gid, url(artist.name), 'Artist name match and both profiles point to following url(s): '||string_agg(u.url, ', ') AS note
INTO discogs_db_artist_link4
FROM u
JOIN artist ON substring(artist.name FROM '(.+?)(?: \(\d+\))?$') = u.name
WHERE u.url = ANY(urls)
GROUP BY u.gid, u.name, artist.name;
