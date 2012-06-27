DROP TABLE IF EXISTS discogs_db_release_cleanup;

WITH multi AS (
	SELECT id
	FROM mb_release_link
	WHERE edits_pending = 0
	GROUP BY id
	HAVING count(gid) > 1
), labels AS (
	SELECT releases_labels.release_id, array_agg(releases_labels.label_id) AS label_id, string_agg(releases_labels.label||' '||releases_labels.catno, ', ') AS label
	FROM releases_labels
	GROUP BY releases_labels.release_id
)
SELECT t1.id, 'Wrong link.
MB country: '||t1.country_name||' label: '||t1.label_name||'
Discogs country: '||release_mb_mapping.country||' label: '||labels.label||'
Discogs release linked multiple times.' AS note
INTO discogs_db_release_cleanup
FROM dblink(:dblink, '
	WITH format AS (
		SELECT release.gid, medium_format.id AS format,
			CASE
				WHEN count(medium) = 1 THEN medium_format.name 
				ELSE count(medium)||''x''||medium_format.name
			END AS format_name
		FROM release
		JOIN medium ON medium.release = release.id
		JOIN medium_format ON medium_format.id = medium.format
		GROUP BY release.gid, medium_format.name, medium_format.id
	), label AS (
		SELECT release.gid, array_agg(do_label_link.gid) AS label, string_agg(do_label_link.name||'' ''||release_label.catalog_number, '', '') AS label_name
		FROM release
		JOIN release_label ON release_label.release = release.id
		JOIN do_label_link ON do_label_link.id = release_label.label
		GROUP BY release.gid
	)
	SELECT l_release_url.id, url.url, release.gid, country.id AS country_id, country.name AS country_name, 
		array_agg(format) AS format, string_agg(format_name, '', '') AS format_name,
		label, label_name
	FROM l_release_url
	JOIN release ON release.id = l_release_url.entity0
	JOIN url ON url.id = l_release_url.entity1
	JOIN country ON country.id = release.country
	JOIN format ON format.gid = release.gid
	JOIN label ON label.gid = release.gid
	WHERE l_release_url.link = 6301
	AND l_release_url.edits_pending = 0
	GROUP BY l_release_url.id, url.url, release.gid, country.id, country.name, label, label_name
') AS t1(id integer, url text, gid uuid, country_id integer, country_name text, format integer[], format_name text, label uuid[], label_name text)
JOIN mb_release_link ON (mb_release_link.gid = t1.gid AND mb_release_link.url = t1.url)
JOIN multi ON multi.id = mb_release_link.id
JOIN release ON release.id = mb_release_link.id
JOIN release_mb_mapping ON release_mb_mapping.release_id = release.id
JOIN labels ON labels.release_id = mb_release_link.id
WHERE t1.country_id <> release_mb_mapping.country_id
AND release_mb_mapping.country_id <> 241
AND t1.country_id <> 240
AND t1.country_id <> 241;