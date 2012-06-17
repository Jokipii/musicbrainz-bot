DROP TABLE IF EXISTS discogs_db_artist_link;

WITH rel AS (
	SELECT DISTINCT t1.artist_gid, url(releases_artists.artist_name), 
		t1.release_name||': http://musicbrainz.org/release/'||t1.release_gid AS release
	FROM dblink(:dblink, '
		WITH res AS (
			WITH improper AS (
				SELECT artist_tag.artist, tag.name
				FROM tag
				JOIN artist_tag ON artist_tag.tag = tag.id
				WHERE tag.name = ''improper discogs link''
			)
			SELECT do_release_link.gid AS release_gid, do_release_link.name AS release_name, 
				artist.id AS artist_id, artist.gid AS artist_gid, artist_credit.name
			FROM do_release_link
			JOIN release ON do_release_link.id = release.id
			JOIN artist_credit ON release.artist_credit = artist_credit.id
			JOIN artist_credit_name ON artist_credit_name.artist_credit = artist_credit.id
			JOIN artist ON artist_credit_name.artist = artist.id
			LEFT JOIN do_artist_link ON do_artist_link.id = artist.id
			LEFT JOIN improper ON improper.artist = artist.id
			WHERE artist.id > 1
			AND do_artist_link.url ISNULL
			AND improper.name ISNULL
		)
		SELECT release_gid, release_name, artist_gid, artist_name.name AS artist_name
		FROM res
		JOIN artist_name ON artist_name.id = res.name
	') AS t1(release_gid uuid, release_name text, artist_gid uuid, artist_name text)
	JOIN mb_release_link ON mb_release_link.gid = t1.release_gid
	JOIN releases_artists ON releases_artists.release_id = mb_release_link.id
	WHERE lower(t1.artist_name) = lower(substring(releases_artists.artist_name FROM '(.+?)(?: \(\d+\))?$'))
)
SELECT artist_gid, url, string_agg(rel.release,' , ') AS releases
INTO discogs_db_artist_link
FROM rel
GROUP BY artist_gid, url;
