DROP TABLE IF EXISTS discogs_db_release_link;

WITH rel_catno AS (
    SELECT DISTINCT mb_label_link.id AS label_id, t1.*, releases_labels.release_id
    FROM dblink(:dblink, '
        SELECT label_gid, label_name, catalog_number, gid, name, track_count, 
            format, country, year, status, barcode
        FROM do_release_link_catno
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
)
SELECT gid, 'http://www.discogs.com/release/' || release_id AS url, 'Release from label "'||label_name
    ||'" found on linked Discogs label with same normalized catalog number "'||catalog_number||'", '||
    CASE 
        WHEN barcode ISNULL THEN 'MB barcode missing'
        WHEN do_barcode ISNULL THEN 'Discogs barcode missing'
        ELSE 'with same barcode "'||barcode||'"'
    END
    ||', same number of tracks, same format, same release country, same release year, same status, and '||
    CASE
        WHEN lower(title) = lower(name) THEN 'exactly same'
        ELSE 'similar'
    END 
    ||' release name MusicBrainz: "'||name||'" Discogs: "'||title||'"' AS note, 1 +
    CASE 
        WHEN barcode NOTNULL AND do_barcode NOTNULL THEN 3
        WHEN barcode ISNULL AND do_barcode ISNULL THEN 1
        ELSE 0
    END +
    CASE
        WHEN lower(title) = lower(name) THEN 5
        ELSE 0
    END AS sum
INTO discogs_db_release_link
FROM rel_catno2
ORDER BY sum DESC, label_name, gid;

DELETE FROM discogs_db_release_link
WHERE gid IN (
  SELECT gid FROM discogs_db_release_link
  GROUP BY gid
  HAVING (count(url) > 1)
);