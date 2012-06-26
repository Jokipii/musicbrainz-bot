DROP TABLE IF EXISTS discogs_db_release_cleanup;

WITH multi AS (
	SELECT id
	FROM mb_release_link
	WHERE edits_pending = 0
	GROUP BY id
	HAVING count(gid) > 1
)
SELECT t1.id, 'Discogs release linked multiple times. Wrong release country '||t1.country_name||' vs '||release_mb_mapping.country AS note
INTO discogs_db_release_cleanup
FROM dblink(:dblink, '
	SELECT l_release_url.id, url.url, release.gid, country.id AS country_id, country.name AS country_name
	FROM l_release_url
	JOIN release ON release.id = l_release_url.entity0
	JOIN url ON url.id = l_release_url.entity1
	JOIN country ON country.id = release.country
	WHERE l_release_url.link = 6301
	AND l_release_url.edits_pending = 0
') AS t1(id integer, url text, gid uuid, country_id integer, country_name text)
JOIN mb_release_link ON (mb_release_link.gid = t1.gid AND mb_release_link.url = t1.url)
JOIN multi ON multi.id = mb_release_link.id
JOIN release_mb_mapping ON release_mb_mapping.release_id = mb_release_link.id
WHERE t1.country_id <> release_mb_mapping.country_id
AND release_mb_mapping.country_id <> 241
AND t1.country_id <> 240;