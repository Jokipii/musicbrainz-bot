DROP TABLE IF EXISTS discogs_db_release_group_link;

WITH masters AS (
	SELECT t1.gid, master.id AS url, 
'MB release group: '||t1.name||' by '||mb_artist_link.name||'
Discogs master: '||master.title||' by '||artist.name||' '||mb_artist_link.url||'
Linked artists, release group and master have unique name (in artist context), names match, and both have earliest release on '||t1.year AS note
	FROM dblink(:dblink, '
		WITH masters AS (
			SELECT release_group.gid, release_name.name, do_artist_link.gid AS artist_gid, 
				release_group_meta.first_release_date_year AS year
			FROM do_artist_link
			JOIN artist_credit_name ON artist_credit_name.artist = do_artist_link.id
			JOIN artist_credit ON (artist_credit.id = artist_credit_name.artist_credit 
				AND artist_credit.artist_count = 1)
			JOIN release_group ON (release_group.artist_credit = artist_credit.id 
				AND release_group.gid NOT IN (SELECT gid FROM do_release_group_link))
			JOIN release_name ON release_name.id = release_group.name
			JOIN release_group_meta ON release_group_meta.id = release_group.id
			WHERE do_artist_link.edits_pending = 0
			AND do_artist_link.url NOT IN (SELECT url FROM do_artist_link GROUP BY url HAVING count(gid) > 1)
		)
		SELECT * 
		FROM masters 
		WHERE artist_gid NOT IN (SELECT artist_gid FROM masters GROUP BY artist_gid, name HAVING count(gid) > 1)
	') AS t1(gid uuid, name text, artist_gid uuid, year integer)
	JOIN mb_artist_link ON mb_artist_link.gid = t1.artist_gid
	JOIN artist ON artist.id = mb_artist_link.id
	JOIN masters_artists ON masters_artists.artist_id = artist.id
	JOIN master ON (master.id = masters_artists.master_id 
		AND lower(master.title) = lower(t1.name) AND master.year = t1.year)
)
SELECT gid, url, note
INTO discogs_db_release_group_link
FROM masters
WHERE gid NOT IN (SELECT gid FROM masters GROUP BY gid HAVING count(url) > 1);