DROP TABLE IF EXISTS do_release_link_catno;

WITH r AS (
	SELECT release.id, (array_agg(do_label_link.name))[1] AS label_name, (array_agg(do_label_link.gid))[1] AS label_gid, 
		(array_agg(release_label.catalog_number))[1] AS catalog_number
	FROM do_label_link
	JOIN release_label ON (release_label.label = do_label_link.id AND release_label.catalog_number NOTNULL)
	JOIN release ON (release.id = release_label.release AND release.id NOT IN (SELECT id FROM do_release_link))
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
SELECT label_name, label_gid, catalog_number, gid, release_name.name, track_count, format, country, 
	date_year AS year, status, barcode
INTO do_release_link_catno
FROM r2
JOIN release ON release.id = r2.id
JOIN release_name ON release_name.id = release.name;
