DROP TABLE IF EXISTS discogs_db_label_link;

WITH result AS (
	WITH cats AS (
		SELECT DISTINCT t1.gid, url(releases_labels.label), t1.cat_count, lower(replace(releases_labels.catno, ' ', '')) AS catno
		FROM dblink(:dblink, '
			WITH cats AS (
				SELECT DISTINCT label.gid, lower(label_name.name) AS name, lower(replace(release_label.catalog_number, '' '', '''')) AS cat
				FROM release_label
				JOIN label ON label.id = release_label.label
				JOIN label_name ON label_name.id = label.name
				LEFT JOIN do_label_link ON do_label_link.id = label.id
				WHERE do_label_link.gid ISNULL
				AND release_label.catalog_number NOTNULL
			)
			SELECT gid, name, array_agg(cat) AS cats, count(cat) AS cat_count
			FROM cats
			GROUP BY gid, name
		') AS t1(gid uuid, name text, cats text[], cat_count integer)
		JOIN releases_labels ON lower(releases_labels.label) = t1.name
		WHERE lower(replace(releases_labels.catno, ' ', '')) = ANY(t1.cats)
	), rels AS (
		SELECT DISTINCT t1.gid, url(releases_labels.label), t1.rel_count, release.title
		FROM dblink(:dblink, '
			WITH rels AS (
				SELECT DISTINCT label.gid, lower(label_name.name) AS name, lower(release_name.name) AS release
				FROM release_label
				JOIN label ON label.id = release_label.label
				JOIN label_name ON label_name.id = label.name
				JOIN release ON release.id = release_label.release
				JOIN release_name ON release_name.id = release.name
				LEFT JOIN do_label_link ON do_label_link.id = label.id
				WHERE do_label_link.gid ISNULL
			)
			SELECT gid, name, array_agg(release) AS releases, count(release) AS rel_count
			FROM rels
			GROUP BY gid, name
		') AS t1(gid uuid, name text, releases text[], rel_count integer)
		JOIN releases_labels ON lower(releases_labels.label) = t1.name
		JOIN release ON release.id = releases_labels.release_id
		WHERE lower(release.title) = ANY(t1.releases)
	)
	SELECT gid, url, count(catno)||' shared catalog numbers from total '||cat_count AS note, count(catno) AS score
	FROM cats
	GROUP BY gid, url, cat_count
	UNION
	SELECT gid, url, count(title)||' shared release titles from total '||rel_count AS note, count(title) AS score
	FROM rels
	GROUP BY gid, url, rel_count
)
SELECT gid, url, 'Name match and '||string_agg(note, ' and ') AS note, sum(score)
INTO discogs_db_label_link
FROM result
GROUP BY gid, url
ORDER BY sum(score) DESC;