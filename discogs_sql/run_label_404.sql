SELECT t1.id, t1.gid, t1.url
FROM dblink(:dblink, '
	SELECT l_label_url.id, label.gid, url.url
	FROM l_label_url
	JOIN url ON l_label_url.entity1 = url.id
	JOIN label ON l_label_url.entity0 = label.id
	WHERE l_label_url.link = 27037
	AND l_label_url.edits_pending = 0;
') AS t1(id integer, gid uuid, url text)
LEFT JOIN mb_label_link ON mb_label_link.gid = t1.gid
WHERE mb_label_link.id ISNULL
LIMIT :lim
