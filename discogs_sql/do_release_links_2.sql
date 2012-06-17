DROP TABLE IF EXISTS rel_catno;

WITH mb_release_link_catno AS (
    SELECT DISTINCT t1.* FROM dblink(:dblink, 'SELECT label_gid, label_name, catalog_number, gid, name, track_count, format, country, year, status, barcode FROM do_release_link_catno')
    AS t1(label_gid uuid, label_name text, catalog_number text, gid uuid, name text, track_count integer, format integer, country integer, year smallint, status integer, barcode text)
)
SELECT DISTINCT mb_label_link.id AS label_id, mb_release_link_catno.*, releases_labels.release_id
INTO rel_catno 
FROM mb_release_link_catno
JOIN mb_label_link ON mb_release_link_catno.label_gid = mb_label_link.gid
JOIN label ON mb_label_link.id = label.id
JOIN releases_labels ON label.name = releases_labels.label
WHERE lower(replace(releases_labels.catno, ' ', '')) = lower(replace(mb_release_link_catno.catalog_number, ' ', ''));

DROP TABLE IF EXISTS discogs_db_release_link;
WITH part1 AS (
	WITH rel_catno2 AS (
	    SELECT DISTINCT label_id, label_gid, label_name, catalog_number, gid, name, barcode, release_mb_mapping.* FROM rel_catno
	    JOIN release_identifier ON value = barcode
	    JOIN release_mb_mapping ON rel_catno.release_id = release_mb_mapping.release_id
	    WHERE type = 'Barcode'
	    AND barcode NOTNULL
	    AND value <> ''
	    AND rel_catno.release_id = release_identifier.release_id
	    AND release_mb_mapping.year = rel_catno.year
	    AND release_mb_mapping.status = rel_catno.status
	    AND (rel_catno.format = release_mb_mapping.format_id 
		OR (rel_catno.format = ANY(array[7, 29, 30, 31]) AND release_mb_mapping.format_id = ANY(array[7, 29, 30, 31])))
	    AND rel_catno.country = release_mb_mapping.country_id
	    AND release_mb_mapping.track_count = rel_catno.track_count
	    AND lower(release_mb_mapping.title) != lower(rel_catno.name)
	)
	SELECT gid, 'http://www.discogs.com/release/' || release_id AS url, 'Release from label "'||label_name||'" found on linked Discogs label with same normalized catalog number "'
		||catalog_number||'", with same barcode "'||barcode
		||'", same number of tracks, same format, same release country, same release year, same status, and similar release name MusicBrainz: "' 
		||name||'" Discogs: "'||title||'"' AS note
	FROM rel_catno2
	ORDER BY label_name, gid
), part2 AS (
	WITH rel_catno2 AS (
	    SELECT DISTINCT label_id, label_gid, label_name, catalog_number, gid, name, barcode, release_mb_mapping.* FROM rel_catno
	    JOIN release_mb_mapping ON rel_catno.release_id = release_mb_mapping.release_id
	    WHERE barcode ISNULL
	    AND release_mb_mapping.year = rel_catno.year
	    AND release_mb_mapping.status = rel_catno.status
	    AND (rel_catno.format = release_mb_mapping.format_id 
		OR (rel_catno.format = ANY(array[7, 29, 30, 31]) AND release_mb_mapping.format_id = ANY(array[7, 29, 30, 31])))
	    AND rel_catno.country = release_mb_mapping.country_id
	    AND release_mb_mapping.track_count = rel_catno.track_count
	    AND lower(release_mb_mapping.title) != lower(rel_catno.name)
	)
	SELECT gid, 'http://www.discogs.com/release/' || release_id AS url, 'Release from label "'||label_name||'" found on linked Discogs label with same normalized catalog number "'
		||catalog_number||'", MB barcode missing, same number of tracks, same format, same release country, same release year, same status, and similar release name MusicBrainz: "' 
		||name||'" Discogs: "'||title||'"' AS note
	FROM rel_catno2
	ORDER BY label_name, gid
)
SELECT *
INTO discogs_db_release_link
FROM part1
UNION
SELECT *
FROM part2;

DELETE FROM discogs_db_release_link
WHERE gid IN (
  SELECT gid FROM discogs_db_release_link
  GROUP BY gid
  HAVING (COUNT(gid) > 1)
);

DROP TABLE IF EXISTS rel_catno;
