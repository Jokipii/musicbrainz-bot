DROP TABLE IF EXISTS discogs_db_member_of_band;

WITH links AS (
	WITH gr AS (
		SELECT mb_artist_link.gid AS gid0, explode(groups) AS group
		FROM mb_artist_link
		JOIN artist ON artist.id = mb_artist_link.id
	)
	SELECT gid0, mb_artist_link.gid AS gid1
	FROM gr
	JOIN artist ON artist.name = gr.group
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
		WHERE link_type.name = ''member of band''
		AND l.entity0 IN (SELECT id FROM do_artist_link)
		AND l.entity0 IN (SELECT id FROM do_artist_link)
	') AS t1(gid0 uuid, gid1 uuid)
)
SELECT links.*, 'Both artists are linked to Discogs where "'||ma0.name||'" is member of "'||ma1.name||'".' AS note
INTO discogs_db_member_of_band
FROM links
JOIN mb_artist_link ma0 ON ma0.gid = links.gid0
JOIN mb_artist_link ma1 ON ma1.gid = links.gid1
ORDER BY gid1;
