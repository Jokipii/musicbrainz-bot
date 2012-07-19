WITH art1 AS (
	SELECT artist_name, artist_id, anv, role_name, role_details, track.id AS track_id, track.position AS tracks
	FROM track
	JOIN tracks_extraartists ON tracks_extraartists.track_id = track.id
	WHERE release_id = :release_id
	UNION
	SELECT artist_name, artist_id, anv, NULL AS role_name, NULL AS role_details, track.id AS track_id, track.position AS tracks
	FROM track
	JOIN tracks_artists ON tracks_artists.track_id = track.id
	WHERE release_id = :release_id
	UNION
	SELECT artist_name, artist_id, anv, NULL AS role_name, NULL AS role_details, NULL AS track_id, NULL AS tracks
	FROM releases_artists
	WHERE release_id = :release_id
	UNION
	SELECT artist_name, artist_id, anv, role_name, role_details, NULL AS track_id, tracks
	FROM releases_extraartists
	WHERE release_id = :release_id
), credits AS (
	SELECT DISTINCT artist_id, string_agg('	'||COALESCE(role_name, '')||
		CASE
			WHEN role_details ISNULL THEN ''
			ELSE ' ['||role_details||']'
		END
		||
		CASE
			WHEN role_name ISNULL AND role_details ISNULL AND tracks ISNULL THEN 'Release - '
			WHEN role_name ISNULL AND role_details ISNULL THEN 'Track - '
			ELSE ' - '
		END
		||
		CASE
			WHEN anv ISNULL OR anv = '' THEN artist_name
			ELSE anv
		END
		||
		CASE
			WHEN tracks ISNULL OR tracks = '' THEN ''
			ELSE ' (tracks: '||tracks||')'
		END
		||
		CASE
			WHEN release_link_type_name ISNULL THEN ''
			ELSE ' --MB--> '||release_link_type_name
		END
		||
		CASE
			WHEN link_attribute_name ISNULL THEN ''
			ELSE ' ['||link_attribute_name||']'
		END, '
') AS credits
	FROM art1
	LEFT JOIN roles ON roles.credit = role_name
	GROUP BY artist_id
), art2 AS (
	SELECT DISTINCT artist_name, artist_id
	FROM art1
), art AS (
	SELECT DISTINCT art2.artist_name||' http://www.discogs.com/artist/'||url(art2.artist_name) AS do_name, 
		string_agg('	'||name||' http://musicbrainz.org/artist/'||gid, '
') AS mb_name, count(gid) AS num, string_agg(credits, '
') AS credits
	FROM art2
	LEFT JOIN mb_artist_link ON id = art2.artist_id
	LEFT JOIN credits ON credits.artist_id = art2.artist_id
	GROUP BY art2.artist_name
)
SELECT 'Release:
'||release.title||' http://www.discogs.com/release/'||release.id||
CASE
	WHEN mb_release_link.gid NOTNULL THEN '
	'||mb_release_link.name||' http://musicbrainz.org/release/'||mb_release_link.gid
	ELSE ''
END
||
CASE
	WHEN master.id NOTNULL THEN '

Master:
'||master.title||' http://www.discogs.com/master/'||master.id||
	CASE
		WHEN mb_release_group_link.gid NOTNULL THEN '
	'||mb_release_group_link.name||' http://musicbrainz.org/release-group/'||mb_release_group_link.gid
		ELSE ''
	END
	ELSE ''
END 
||'

Linked artists:
'||CASE WHEN (SELECT count(*) FROM art WHERE num = 1) > 0 THEN (SELECT string_agg(do_name||'
'||mb_name||'
'||credits, '
') FROM art WHERE num = 1 GROUP BY num) ELSE '' END
||'

'||
'Multi-linked artists:
'||CASE WHEN (SELECT count(*) FROM art WHERE num > 1) > 0 THEN (SELECT string_agg(do_name||'
'||mb_name||'
'||credits, '
') FROM art WHERE num > 1 GROUP BY num) ELSE '' END
||'

'||
'Unlinked artists:
'||CASE WHEN (SELECT count(*) FROM art WHERE mb_name ISNULL) > 0 THEN (SELECT string_agg(do_name||'
'||credits, '
') FROM art WHERE mb_name ISNULL GROUP BY num) ELSE '' END
AS note
FROM release
LEFT JOIN mb_release_link ON mb_release_link.id = release.id
LEFT JOIN master ON master.id = release.master_id
LEFT JOIN mb_release_group_link ON mb_release_group_link.id = master.id
WHERE release.id = :release_id 
