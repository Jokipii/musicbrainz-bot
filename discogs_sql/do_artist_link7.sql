DROP TABLE IF EXISTS discogs_db_artist_link7;

WITH res AS (
	SELECT DISTINCT t1.gid, url(releases_artists.artist_name), release.title
	FROM dblink(:dblink, '
		WITH rel AS (
			SELECT release.id
			FROM release
			EXCEPT
			SELECT DISTINCT release.id
			FROM do_artist_link
			JOIN artist ON artist.id = do_artist_link.id
			JOIN artist_credit_name ON artist_credit_name.artist = artist.id
			JOIN artist_credit ON artist_credit.id = artist_credit_name.artist_credit
			JOIN release ON release.artist_credit = artist_credit.id
			WHERE artist.id <> 1
		)
		SELECT s_artist.name, s_artist.gid, array_agg(lower(release_name.name)) AS releases
		FROM rel
		JOIN release ON release.id = rel.id
		JOIN release_name ON release_name.id = release.name
		JOIN artist_credit ON artist_credit.id = release.artist_credit
		JOIN artist_credit_name ON artist_credit_name.artist_credit = artist_credit.id
		JOIN s_artist ON s_artist.id = artist_credit_name.artist
		WHERE s_artist.id <> 1
		GROUP BY s_artist.name, s_artist.gid
		HAVING count(rel.id) >= 2
	') AS t1(name text, gid uuid, releases text[])
	JOIN releases_artists ON lower(substring(releases_artists.artist_name FROM '(.+?)(?: \(\d+\))?$')) = lower(t1.name)
	JOIN release ON release.id = releases_artists.release_id
	WHERE lower(release.title) = ANY(t1.releases)
)
SELECT gid, url, 'Name match and both profiles have following releases: '||string_agg(title, ', ') AS note
INTO discogs_db_artist_link7
FROM res
GROUP BY gid, url
HAVING count(title) >= 2
ORDER BY count(title) DESC;
