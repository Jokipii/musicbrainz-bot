DROP TABLE IF EXISTS discogs_db_artist_link6;

WITH res AS (
	SELECT t1.gid, url(t1.name), t1.name||' perform as '||t1.perf_name||' http://musicbrainz.org/artist/'
		||t1.perf_gid||' that is linked to Discogs. Discogs artist points to artist with same realname' AS note
	FROM dblink(:dblink, '
		SELECT a0.gid, a0.name, a1.gid AS perf_gid, a1.name AS perf_name
		FROM l_artist_artist
		JOIN s_artist a0 ON a0.id = l_artist_artist.entity0
		JOIN s_artist a1 ON a1.id = l_artist_artist.entity1
		JOIN link ON link.id = l_artist_artist.link
		JOIN link_type ON link_type.id = link.link_type
		WHERE link_type.name = ''is person''
		AND a0.id NOT IN (SELECT id FROM do_artist_link)
		AND a1.id IN (SELECT id FROM do_artist_link)
	') AS t1(gid uuid, name text, perf_gid uuid, perf_name text)
	JOIN mb_artist_link ON mb_artist_link.gid = t1.perf_gid
	JOIN artist ON artist.id = mb_artist_link.id
	WHERE t1.name = ANY(artist.aliases)
	AND t1.name = artist.realname
	UNION
	SELECT t1.perf_gid AS gid , url(t1.perf_name), t1.perf_name||' is performer name of '||t1.name||' http://musicbrainz.org/artist/'
		||t1.gid||' that is linked to Discogs. Discogs artist points to artist with same performer name' AS note
	FROM dblink(:dblink, '
		SELECT a0.gid, a0.name, a1.gid AS perf_gid, a1.name AS perf_name
		FROM l_artist_artist
		JOIN s_artist a0 ON a0.id = l_artist_artist.entity0
		JOIN s_artist a1 ON a1.id = l_artist_artist.entity1
		JOIN link ON link.id = l_artist_artist.link
		JOIN link_type ON link_type.id = link.link_type
		WHERE link_type.name = ''is person''
		AND a0.id IN (SELECT id FROM do_artist_link)
		AND a1.id NOT IN (SELECT id FROM do_artist_link)
	') AS t1(gid uuid, name text, perf_gid uuid, perf_name text)
	JOIN mb_artist_link ON mb_artist_link.gid = t1.gid
	JOIN artist ON artist.id = mb_artist_link.id
	WHERE t1.perf_name = ANY(artist.aliases)
)
SELECT res.gid, res.url, string_agg(res.note, '
') AS note
INTO discogs_db_artist_link6
FROM res
GROUP BY res.gid, res.url
