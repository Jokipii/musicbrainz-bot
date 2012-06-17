DROP TABLE IF EXISTS discogs_db_perform_as;

WITH links AS (
	WITH al AS (
		SELECT mb_artist_link.gid AS gid0, explode(aliases) AS alias
		FROM mb_artist_link
		JOIN artist ON artist.id = mb_artist_link.id
		WHERE substring(artist.name FROM '(.+?)(?: \(\d+\))?$') = artist.realname
	)
	SELECT gid0, mb_artist_link.gid AS gid1
	FROM al
	JOIN artist ON artist.name = al.alias
	JOIN mb_artist_link ON mb_artist_link.id = artist.id

	EXCEPT

	SELECT * 
	FROM dblink(:dblink, '
		SELECT a0.gid AS gid0, a1.gid AS gid1
		FROM l_artist_artist l
		JOIN artist a0 ON a0.id = l.entity0
		JOIN artist a1 ON a1.id = l.entity1
		JOIN link ON link.id = l.link
		JOIN link_type ON link_type.id = link.link_type
		WHERE link_type.name = ''is person''
		AND l.entity0 IN (SELECT id FROM do_artist_link)
		AND l.entity0 IN (SELECT id FROM do_artist_link)
	') AS t1(gid0 uuid, gid1 uuid)
)
SELECT links.*, 'Both artists are linked to Discogs where "'||ma0.name||'" '||ma0.url||' performs as "'||ma1.name||'" '||ma1.url AS note
INTO discogs_db_perform_as
FROM links
JOIN mb_artist_link ma0 ON ma0.gid = links.gid0
JOIN mb_artist_link ma1 ON ma1.gid = links.gid1
ORDER BY gid0;
