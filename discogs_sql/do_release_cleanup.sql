DROP TABLE IF EXISTS discogs_db_release_cleanup;

WITH rel_catno AS (
	SELECT DISTINCT mb_label_link.id AS label_id, t1.*, releases_labels.release_id
	FROM dblink(:dblink, '
		WITH r AS (
			SELECT release.id, (array_agg(do_label_link.name))[1] AS label_name, (array_agg(do_label_link.gid))[1] AS label_gid, 
				(array_agg(release_label.catalog_number))[1] AS catalog_number
			FROM do_label_link
			JOIN release_label ON (release_label.label = do_label_link.id AND release_label.catalog_number NOTNULL)
			JOIN release ON (release.id = release_label.release AND release.id IN (
				SELECT id
				FROM do_release_link
				GROUP BY id
				HAVING count(url) > 1))
			GROUP BY release.id
			HAVING count(do_label_link.gid) = 1
		), r2 AS (
			SELECT r.id, label_name, label_gid, catalog_number, sum(tracklist.track_count) as track_count, 
				(array_agg(medium.format))[1] AS format
			FROM r
			JOIN medium ON medium.release = r.id
			JOIN tracklist ON tracklist.id = medium.tracklist
			GROUP BY r.id, label_name, label_gid, catalog_number
			HAVING count(medium.format) = 1
		)
		SELECT label_gid, label_name, catalog_number, gid, release_name.name, track_count, format, country, 
			date_year AS year, status, barcode
		FROM r2
		JOIN release ON release.id = r2.id
		JOIN release_name ON release_name.id = release.name;
	') AS t1(label_gid uuid, label_name text, catalog_number text, gid uuid, name text, 
		track_count integer, format integer, country integer, year smallint, status integer, barcode text)
	JOIN mb_label_link ON t1.label_gid = mb_label_link.gid
	JOIN label ON mb_label_link.id = label.id
	JOIN releases_labels ON label.name = releases_labels.label
	WHERE lower(replace(releases_labels.catno, ' ', '')) = lower(replace(t1.catalog_number, ' ', ''))
), rel_catno2 AS (
	SELECT DISTINCT label_id, label_gid, label_name, catalog_number, gid, name, barcode, 
		release_mb_mapping.*, release_identifier.value AS do_barcode
	FROM rel_catno
	LEFT JOIN release_identifier ON (barcode NOTNULL AND type = 'Barcode' AND value = barcode 
		AND value <> '' AND rel_catno.release_id = release_identifier.release_id)
	JOIN release_mb_mapping ON rel_catno.release_id = release_mb_mapping.release_id
	WHERE release_mb_mapping.year = rel_catno.year
	AND release_mb_mapping.status = rel_catno.status
	AND (rel_catno.format = release_mb_mapping.format_id 
	OR (rel_catno.format = ANY(array[7, 29, 30, 31]) AND release_mb_mapping.format_id = ANY(array[7, 29, 30, 31])))
	AND rel_catno.country = release_mb_mapping.country_id
	AND release_mb_mapping.track_count = rel_catno.track_count
), multi_url AS (
	SELECT *
	FROM mb_release_link
	WHERE gid IN (
		SELECT gid
		FROM mb_release_link
		GROUP BY gid
		HAVING count(url) > 1)
), found AS (
	SELECT gid, url
	FROM multi_url
	INTERSECT
	SELECT gid, 'http://www.discogs.com/release/'||release_id AS url
	FROM rel_catno2
	WHERE gid IN (
		SELECT gid
		FROM rel_catno2
		GROUP BY gid
		HAVING count(release_id) = 1)
), removable AS (
	SELECT gid, url
	FROM multi_url
	WHERE gid IN (SELECT gid FROM found)
	EXCEPT
	SELECT gid, url
	FROM found
), remove AS (
	SELECT t1.*
	FROM dblink(:dblink, '
		WITH uniq AS (
				SELECT id
				FROM do_release_link
				GROUP BY id
				HAVING count(url) > 1
		)
		SELECT DISTINCT l_release_url.id, do_release_link.gid, url.url
		FROM do_release_link
		JOIN l_release_url ON (do_release_link.id IN (select id FROM uniq) AND l_release_url.entity0 = do_release_link.id)
		JOIN url ON (url.id = l_release_url.entity1 AND url.url = do_release_link.url)
	') AS t1(id integer, gid uuid, url text)
	JOIN removable ON removable.gid = t1.gid AND removable.url = t1.url
)
SELECT remove.id, 'Release has multiple Discogs urls.
http://www.discogs.com/release/'||release_id||' is only one that has matching label "'||label_name
	    ||'", same normalized catalog number "'||catalog_number||'"'||
	    CASE 
		WHEN barcode NOTNULL AND do_barcode NOTNULL THEN ', same barcode "'||barcode||'"'
		ELSE ''
	    END
	    ||', same number of tracks, same format, same release country, same release year, and same status.

Removing others.' AS note
INTO discogs_db_release_cleanup
FROM remove
JOIN found ON found.gid = remove.gid
JOIN rel_catno2 ON rel_catno2.gid = remove.gid
ORDER BY remove.gid