WITH countries AS (
	SELECT t1.* 
	FROM dblink(:dblink, '
		SELECT id, array_cat(array[lower(name)||''%%''],country_search.search) AS search FROM country 
		LEFT JOIN country_search ON country.iso_code = country_search.iso_code
	') AS t1(id integer, search text[])
), gids AS (
	SELECT t1.* 
	FROM dblink(:dblink, '
		SELECT do_artist_link.gid 
		FROM do_artist_link
		JOIN artist ON artist.id = do_artist_link.id
		WHERE artist.country ISNULL
	') AS t1(gid uuid)
)
SELECT countries.id AS country_id, mb_artist_link.gid, mb_artist_link.name, substring(artist.profile FROM '^[^.]+\.?') AS comment
FROM mb_artist_link
JOIN artist ON mb_artist_link.id = artist.id
JOIN gids ON gids.gid = mb_artist_link.gid
JOIN countries ON lower(artist.profile) LIKE ANY(search)
WHERE artist.profile NOT LIKE '%%?%%'
AND artist.profile !~* '^[a-z]+[-/]'
LIMIT :limit
