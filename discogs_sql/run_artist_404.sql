SELECT t1.id, t1.gid, t1.url
FROM dblink(:dblink, '
	SELECT l_artist_url.id, artist.gid, url.url
	FROM l_artist_url
	JOIN url ON l_artist_url.entity1 = url.id
	JOIN artist ON l_artist_url.entity0 = artist.id
	WHERE l_artist_url.link = 26038
	AND l_artist_url.edits_pending = 0;
') AS t1(id integer, gid uuid, url text)
LEFT JOIN mb_artist_link ON mb_artist_link.gid = t1.gid
WHERE mb_artist_link.id ISNULL
LIMIT :lim
