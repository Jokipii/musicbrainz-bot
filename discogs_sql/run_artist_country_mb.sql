WITH countries AS (
	SELECT id, array_cat(array[lower(name)||'%%'],country_search.search) AS search FROM country 
	LEFT JOIN country_search ON country.iso_code = country_search.iso_code
)
SELECT countries.id AS country_id, s_artist.gid, s_artist.name, comment FROM s_artist
JOIN countries ON lower(comment) LIKE ANY(search)
AND country ISNULL
AND comment NOT LIKE '%%?%%'
AND comment !~* '^[a-z]+[-/-]'
LIMIT %s
