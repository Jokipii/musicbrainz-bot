DROP TABLE IF EXISTS discogs_db_release_barcode;

WITH res AS (
	SELECT DISTINCT mb_release_link.gid, mb_release_link.url, release_identifier.value 
	FROM dblink(:dblink, '
		WITH gids AS (
			SELECT gid
			FROM do_release_link
			GROUP BY gid
			HAVING count(url) = 1
		)
		SELECT release.gid, release.country, release.date_year, lower(replace(release_label.catalog_number, '' '', '''')), medium.format
		FROM release
		JOIN gids ON release.gid = gids.gid
		JOIN medium ON medium.release = release.id
		LEFT JOIN release_label ON release_label.release = release.id
		AND release.barcode ISNULL
		AND release.quality = -1
	') AS t1(gid uuid, country integer, date_year integer, catalog_number text, format integer)
	JOIN mb_release_link ON mb_release_link.gid = t1.gid
	JOIN release_mb_mapping ON release_mb_mapping.release_id = mb_release_link.id
	JOIN release_identifier ON release_identifier.release_id = mb_release_link.id
	JOIN releases_labels ON releases_labels.release_id = mb_release_link.id
	WHERE release_mb_mapping.country_id = t1.country
	AND release_mb_mapping.year = t1.date_year
	AND release_identifier.type = 'Barcode'
	AND release_mb_mapping.format_id = t1.format
	AND lower(replace(releases_labels.catno, ' ', '')) = t1.catalog_number
)
SELECT * 
INTO discogs_db_release_barcode
FROM res
WHERE value ~ '^[0-9]{12,13}$';
