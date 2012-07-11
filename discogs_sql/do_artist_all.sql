DROP TABLE IF EXISTS discogs_db_artist_all;

WITH evidence AS (
	WITH res2 AS (
		SELECT t1.*, explode(artist.members) AS do_name
		FROM dblink(:dblink, '
			SELECT a0.gid, a0.name, a1.gid AS band_gid, a1.name AS band_name
			FROM l_artist_artist
			JOIN s_artist a0 ON a0.id = l_artist_artist.entity0
			JOIN s_artist a1 ON a1.id = l_artist_artist.entity1
			JOIN link ON link.id = l_artist_artist.link
			JOIN link_type ON link_type.id = link.link_type
			WHERE link_type.name = ''member of band''
			AND a0.id NOT IN (SELECT id FROM do_artist_link)
			AND a1.id IN (SELECT id FROM do_artist_link)
		') AS t1(gid uuid, name text, band_gid uuid, band_name text)
		JOIN mb_artist_link ON mb_artist_link.gid = t1.band_gid
		JOIN artist ON artist.id = mb_artist_link.id
	), u AS (
		SELECT * FROM dblink(:dblink, '
			SELECT s_artist.gid, s_artist.name, url.url
			FROM s_artist
			JOIN l_artist_url ON l_artist_url.entity0 = s_artist.id
			JOIN url ON url.id = l_artist_url.entity1
			AND s_artist.id NOT IN (SELECT id FROM do_artist_link)
		') AS t1(gid uuid, name text, url text)
	), aa AS (
		SELECT t1.*, explode(artist.groups) AS do_group, mb_artist_link.url
		FROM dblink(:dblink, '
			SELECT a0.gid, a0.name, a1.gid AS band_gid, a1.name AS band_name
			FROM l_artist_artist
			JOIN s_artist a0 ON a0.id = l_artist_artist.entity0
			JOIN s_artist a1 ON a1.id = l_artist_artist.entity1
			JOIN link ON link.id = l_artist_artist.link
			JOIN link_type ON link_type.id = link.link_type
			WHERE link_type.name = ''member of band''
			AND a0.id IN (SELECT id FROM do_artist_link)
			AND a1.id NOT IN (SELECT id FROM do_artist_link)
		') AS t1(gid uuid, name text, band_gid uuid, band_name text)
		JOIN mb_artist_link ON mb_artist_link.gid = t1.gid
		JOIN artist ON artist.id = mb_artist_link.id
	), res3 AS (
		SELECT DISTINCT t1.gid, url(releases_artists.artist_name), release.title
		FROM dblink(:dblink, '
			WITH rel AS (
				SELECT release.id
				FROM release
				EXCEPT
				SELECT DISTINCT release.id
				FROM do_artist_link
				JOIN artist ON artist.id = do_artist_link.id
				JOIN artist_credit_name ON artist_credit_name.artist = artist.id
				JOIN artist_credit ON artist_credit.id = artist_credit_name.artist_credit
				JOIN release ON release.artist_credit = artist_credit.id
				WHERE artist.id <> 1
			)
			SELECT s_artist.name, s_artist.gid, array_agg(lower(release_name.name)) AS releases
			FROM rel
			JOIN release ON release.id = rel.id
			JOIN release_name ON release_name.id = release.name
			JOIN artist_credit ON artist_credit.id = release.artist_credit
			JOIN artist_credit_name ON artist_credit_name.artist_credit = artist_credit.id
			JOIN s_artist ON s_artist.id = artist_credit_name.artist
			WHERE s_artist.id <> 1
			GROUP BY s_artist.name, s_artist.gid
			HAVING count(rel.id) >= 2
		') AS t1(name text, gid uuid, releases text[])
		JOIN releases_artists ON lower(substring(releases_artists.artist_name FROM '(.+?)(?: \(\d+\))?$')) = lower(t1.name)
		JOIN release ON release.id = releases_artists.release_id
		WHERE lower(release.title) = ANY(t1.releases)
	), tracks AS (
		WITH ev AS (
			SELECT DISTINCT * FROM discogs_db_artist_evidence_track
		)
		SELECT gid, url, array_agg(note) AS note, sum(score)
		FROM ev
		GROUP BY gid, url
	)


	SELECT DISTINCT t1.artist_gid AS gid, url(releases_artists.artist_name), 
		'Release with Discogs link points to same artist: '||t1.release_name||': http://musicbrainz.org/release/'||t1.release_gid AS note,
		12 AS score
	FROM dblink(:dblink, '
		WITH improper AS (
			SELECT artist_tag.artist, tag.name
			FROM tag
			JOIN artist_tag ON artist_tag.tag = tag.id
			WHERE tag.name = ''improper discogs link''
		)
		SELECT do_release_link.gid AS release_gid, do_release_link.name AS release_name, 
			artist.gid AS artist_gid, artist_name.name AS artist_name
		FROM do_release_link
		JOIN release ON do_release_link.id = release.id
		JOIN artist_credit ON release.artist_credit = artist_credit.id
		JOIN artist_credit_name ON artist_credit_name.artist_credit = artist_credit.id
		JOIN artist ON artist_credit_name.artist = artist.id
		LEFT JOIN do_artist_link ON do_artist_link.id = artist.id
		JOIN artist_name ON artist_name.id = artist.name
		WHERE artist.id <> 1
        AND do_release_link.edits_pending = 0
		AND artist.id NOT IN (SELECT artist FROM improper)
		AND do_artist_link.url ISNULL
	') AS t1(release_gid uuid, release_name text, artist_gid uuid, artist_name text)
	JOIN mb_release_link ON mb_release_link.gid = t1.release_gid
	JOIN releases_artists ON releases_artists.release_id = mb_release_link.id
	WHERE lower(t1.artist_name) = lower(substring(releases_artists.artist_name FROM '(.+?)(?: \(\d+\))?$'))

	UNION

	SELECT t1.artist_gid AS gid, url(masters_artists.artist_name), 'Linked Release Group "'||t1.name
		||'" http://www.musicbrainz.org/release-group/'||t1.gid||' points to same artist' AS note, 12 AS score
	FROM dblink(:dblink, '
		SELECT do_release_group_link.gid, do_release_group_link.name, artist.gid AS artist_gid, artist_name.name AS artist_name
		FROM do_release_group_link
		JOIN release_group ON release_group.id = do_release_group_link.id
		JOIN artist_credit ON artist_credit.id = release_group.artist_credit
		JOIN artist_credit_name ON artist_credit_name.artist_credit = artist_credit.id
		JOIN artist ON artist.id = artist_credit_name.artist
		LEFT JOIN do_artist_link ON do_artist_link.id = artist.id
		JOIN artist_name ON artist_name.id = artist.name
		WHERE artist_credit.artist_count = 1
        AND do_release_group_link.edits_pending = 0
		AND artist_credit.id <> 1
		AND do_artist_link.gid ISNULL
	') AS t1(gid uuid, name text, artist_gid uuid, artist_name text)
	JOIN mb_release_group_link ON mb_release_group_link.gid = t1.gid
	JOIN master ON master.id = mb_release_group_link.id
	JOIN masters_artists ON masters_artists.master_id = master.id
	WHERE lower(t1.artist_name) = lower(substring(masters_artists.artist_name FROM '(.+?)(?: \(\d+\))?$'))

	UNION

	SELECT gid, url(do_name), name||' is member of band '||band_name||' http://musicbrainz.org/artist/'
	||band_gid||' that is linked to Discogs, linked band has member with name match.' AS note, 3 as score
	FROM res2
	WHERE lower(name) = lower(substring(do_name FROM '(.+?)(?: \(\d+\))?$'))

	UNION

	SELECT u.gid, url(artist.name), 'Both profiles point to following url(s): '||string_agg(u.url, ', ') AS note, 3*count(u.url) as score
	FROM u
	JOIN artist ON lower(substring(artist.name FROM '(.+?)(?: \(\d+\))?$')) = lower(u.name)
	WHERE u.url = ANY(urls)
	GROUP BY u.gid, u.name, artist.name

	UNION

	SELECT aa.band_gid AS gid, url(do_group), aa.band_name||' member '||aa.name||' is linked to Discogs, linked Discogs artist '
		||aa.url||' is member of band with name match.' AS note, 3 as score
	FROM aa
	WHERE lower(band_name) = lower(substring(do_group FROM '(.+?)(?: \(\d+\))?$'))

	UNION

	SELECT t1.gid, url(t1.name), t1.name||' perform as '||t1.perf_name||' http://musicbrainz.org/artist/'
		||t1.perf_gid||' that is linked to Discogs. Discogs artist points to artist with same realname' AS note, 3 AS score
	FROM dblink(:dblink, '
		SELECT a0.gid, a0.name, a1.gid AS perf_gid, a1.name AS perf_name
		FROM l_artist_artist
		JOIN s_artist a0 ON a0.id = l_artist_artist.entity0
		JOIN s_artist a1 ON a1.id = l_artist_artist.entity1
		JOIN link ON link.id = l_artist_artist.link
		JOIN link_type ON link_type.id = link.link_type
		WHERE link_type.name = ''is person''
		AND a0.id NOT IN (SELECT id FROM do_artist_link)
		AND a1.id IN (SELECT id FROM do_artist_link)
	') AS t1(gid uuid, name text, perf_gid uuid, perf_name text)
	JOIN mb_artist_link ON mb_artist_link.gid = t1.perf_gid
	JOIN artist ON artist.id = mb_artist_link.id
	WHERE t1.name = ANY(artist.aliases)
	AND t1.name = artist.realname

	UNION

	SELECT t1.perf_gid AS gid , url(t1.perf_name), t1.perf_name||' is performer name of '||t1.name||' http://musicbrainz.org/artist/'
		||t1.gid||' that is linked to Discogs. Discogs artist points to artist with same performer name' AS note, 3 AS score
	FROM dblink(:dblink, '
		SELECT a0.gid, a0.name, a1.gid AS perf_gid, a1.name AS perf_name
		FROM l_artist_artist
		JOIN s_artist a0 ON a0.id = l_artist_artist.entity0
		JOIN s_artist a1 ON a1.id = l_artist_artist.entity1
		JOIN link ON link.id = l_artist_artist.link
		JOIN link_type ON link_type.id = link.link_type
		WHERE link_type.name = ''is person''
		AND a0.id IN (SELECT id FROM do_artist_link)
		AND a1.id NOT IN (SELECT id FROM do_artist_link)
	') AS t1(gid uuid, name text, perf_gid uuid, perf_name text)
	JOIN mb_artist_link ON mb_artist_link.gid = t1.gid
	JOIN artist ON artist.id = mb_artist_link.id
	WHERE t1.perf_name = ANY(artist.aliases)

	UNION

	SELECT gid, url, 'Both profiles have following releases: '||string_agg(title, ', ') AS note, 4*count(title) AS score
	FROM res3
	GROUP BY gid, url
	HAVING count(title) >= 2

	UNION

	SELECT gid, url,
		CASE 
		WHEN sum = 1 THEN note[1]
		WHEN sum = 2 THEN note[1]||'
'||note[2]
		WHEN sum = 3 THEN note[1]||'
'||note[2]||'
'||note[3]
		ELSE note[1]||'
'||note[2]||'
'||note[3]||'
and '||sum-3||' more similary linked tracks pointing to same artist'
		END AS note, sum AS score
	FROM tracks

)
SELECT gid, url, 'Found Discogs Artist with name match.
'||string_agg(note, '
') AS note, sum(score)
INTO discogs_db_artist_all
FROM evidence
GROUP BY gid, url
ORDER BY sum(score) DESC;
