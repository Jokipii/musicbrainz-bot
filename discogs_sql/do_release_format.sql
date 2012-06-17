DROP TABLE IF EXISTS discogs_db_release_format;

WITH a AS (
	WITH rels AS (
		SELECT t1.*, mb_release_link.id, mb_release_link.url FROM dblink(:dblink, '
			SELECT release.gid, release.date_year, release.country
			FROM do_release_link
			JOIN release ON release.id = do_release_link.id
			JOIN medium ON medium.release = release.id
			WHERE format ISNULL
			AND do_release_link.edits_pending = 0
			GROUP BY release.gid, release.date_year, release.country
			HAVING count(url) = 1
		') AS t1(gid uuid, year integer, country integer)
		JOIN mb_release_link ON mb_release_link.gid = t1.gid
	)
	SELECT DISTINCT rels.gid, release_mb_mapping.format_id, rels.url
	FROM rels
	JOIN release_mb_mapping ON release_mb_mapping.release_id = rels.id
	WHERE rels.country = release_mb_mapping.country_id
	AND (rels.year ISNULL OR rels.year = release_mb_mapping.year)
), b AS (
	SELECT gid FROM a GROUP BY gid HAVING count(format_id) = 1
)
SELECT a.*
INTO discogs_db_release_format
FROM b
JOIN a ON a.gid = b.gid;
