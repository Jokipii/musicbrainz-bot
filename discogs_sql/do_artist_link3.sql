DROP TABLE IF EXISTS discogs_db_artist_link3;

WITH results AS (
	WITH res AS (
		WITH aa AS (
			SELECT * FROM dblink(:dblink, '
				SELECT a0.gid, a0.name, a1.gid AS band_gid, a1.name AS band_name
				FROM l_artist_artist
				JOIN s_artist a0 ON a0.id = l_artist_artist.entity0
				JOIN s_artist a1 ON a1.id = l_artist_artist.entity1
				JOIN link ON link.id = l_artist_artist.link
				JOIN link_type ON link_type.id = link.link_type
				WHERE link_type.name = ''member of band''
				AND a0.id NOT IN (SELECT id FROM do_artist_link)
				AND a1.id IN (SELECT id FROM do_artist_link)
			') AS t1(gid uuid, name text, band_gid uuid, band_name text)
		)
		SELECT aa.*, explode(artist.members) AS do_name
		FROM aa
		JOIN mb_artist_link ON mb_artist_link.gid = aa.band_gid
		JOIN artist ON artist.id = mb_artist_link.id
	)
	SELECT gid, url(do_name), name||' is member of band '||band_name||' http://musicbrainz.org/artist/'
        ||band_gid||' that is linked to Discogs, linked band has member with name match.' AS note
	FROM res
	WHERE lower(name) = lower(substring(do_name FROM '(.+?)(?: \(\d+\))?$'))
)
SELECT gid, url, string_agg(note, '
') AS note
INTO discogs_db_artist_link3
FROM results
GROUP BY gid, url;
