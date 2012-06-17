DROP TABLE IF EXISTS do_release_link_catno;

WITH r AS (
    SELECT DISTINCT do_label_link.name AS label_name, do_label_link.gid AS label_gid, release_label.catalog_number, release.gid, 
        release_name.name, tracklist.track_count, medium.format, release.country, release.date_year AS year, 
        release.status, tracklist.id, release.barcode
    FROM do_label_link
    JOIN release_label ON do_label_link.id = release_label.label
    JOIN release ON release_label.release = release.id
    JOIN release_name ON release.name = release_name.id
    JOIN medium ON medium.release = release.id
    JOIN tracklist ON medium.tracklist = tracklist.id
    WHERE release_label.catalog_number NOTNULL
)
SELECT label_name, label_gid, catalog_number, gid, name, SUM(track_count) AS track_count, format, country, year, status, barcode
INTO do_release_link_catno
FROM r
GROUP BY label_name, label_gid, catalog_number, gid, name, format, country, year, status, barcode;

DELETE FROM do_release_link_catno USING do_release_link WHERE do_release_link_catno.gid = do_release_link.gid;
