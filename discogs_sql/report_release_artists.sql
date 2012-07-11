WITH art AS (
	SELECT DISTINCT artist_name, artist_id
	FROM track
	JOIN tracks_extraartists ON tracks_extraartists.track_id = track.id
	WHERE release_id = :release_id
	UNION
	SELECT DISTINCT artist_name, artist_id
	FROM track
	JOIN tracks_artists ON tracks_artists.track_id = track.id
	WHERE release_id = :release_id
	UNION
	SELECT DISTINCT artist_name, artist_id
	FROM releases_artists
	WHERE release_id = :release_id
	UNION
	SELECT DISTINCT artist_name, artist_id
	FROM releases_extraartists
	WHERE release_id = :release_id
), unlinked AS (
	SELECT DISTINCT art.artist_name, url(art.artist_name)
	FROM art
	LEFT JOIN mb_artist_link ON mb_artist_link.id = artist_id
	WHERE mb_artist_link.gid ISNULL
	ORDER BY art.artist_name
), multi AS (
	SELECT DISTINCT art.artist_name, url(art.artist_name), 'MB artists pointing here: '
        ||string_agg('http://www.musicbrainz.org/artist/'||mb_artist_link.gid, ' ') AS note
	FROM art
	LEFT JOIN mb_artist_link ON mb_artist_link.id = artist_id
	GROUP BY art.artist_name
	HAVING count(mb_artist_link.gid) > 1
	ORDER BY art.artist_name
)
SELECT 'Unlinked artists:
'||string_agg(artist_name||' http://www.discogs.com/artist/'||url, '
') AS note
FROM unlinked
UNION
SELECT 'Multi-linked artists:
'||string_agg(artist_name||' http://www.discogs.com/artist/'||url||' '||note, '
') AS note
FROM multi
