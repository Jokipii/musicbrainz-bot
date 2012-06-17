DROP TABLE IF EXISTS discogs_db_label_link;

SELECT t1.gid, url(releases_labels.label), 'Name match and '||count(releases_labels.catno)|| ' shared catalog numbers.' AS note
INTO discogs_db_label_link
FROM dblink(:dblink, '
	SELECT label.gid, lower(label_name.name), array_agg(lower(replace(release_label.catalog_number, '' '', ''''))) AS cats
	FROM release_label
	JOIN label ON label.id = release_label.label
	JOIN label_name ON label_name.id = label.name
	LEFT JOIN do_label_link ON do_label_link.id = label.id
	WHERE do_label_link.gid ISNULL
	GROUP BY label.gid, label_name.name
	ORDER BY count(release_label.catalog_number) DESC
') AS t1(gid uuid, name text, cats text[])
JOIN releases_labels ON lower(releases_labels.label) = t1.name
WHERE lower(replace(releases_labels.catno, ' ', '')) = ANY(t1.cats)
GROUP BY t1.gid, releases_labels.label
ORDER BY count(releases_labels.catno) DESC
