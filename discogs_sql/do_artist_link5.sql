DROP TABLE IF EXISTS discogs_db_artist_link5;

WITH results AS (
	WITH aa AS (
		SELECT t1.*, explode(artist.groups) AS do_group, mb_artist_link.url
		FROM dblink(:dblink, '
			SELECT a0.gid, a0.name, a1.gid AS band_gid, a1.name AS band_name
			FROM l_artist_artist
			JOIN s_artist a0 ON a0.id = l_artist_artist.entity0
			JOIN s_artist a1 ON a1.id = l_artist_artist.entity1
			JOIN link ON link.id = l_artist_artist.link
			JOIN link_type ON link_type.id = link.link_type
			WHERE link_type.name = ''member of band''
			AND a0.id IN (SELECT id FROM do_artist_link)
			AND a1.id NOT IN (SELECT id FROM do_artist_link)
		') AS t1(gid uuid, name text, band_gid uuid, band_name text)
		JOIN mb_artist_link ON mb_artist_link.gid = t1.gid
		JOIN artist ON artist.id = mb_artist_link.id
	)
	SELECT aa.band_gid AS gid, url(do_group), aa.band_name||' member '||aa.name||' is linked to Discogs, linked Discogs artist '
		||aa.url||' is member of band with name match.' AS note
	FROM aa
	WHERE lower(band_name) = lower(substring(do_group FROM '(.+?)(?: \(\d+\))?$'))

)
SELECT gid, url, string_agg(note, '
') AS note
INTO discogs_db_artist_link5
FROM results
GROUP BY gid, url;
