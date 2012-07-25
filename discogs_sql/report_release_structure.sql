INSERT INTO update_log(updated, event) VALUES ('report_release_structure', 'started id: '||:release_id);

WITH mb_work AS (
	SELECT *
	FROM dblink(:dblink, '
		SELECT release.gid AS release_gid, recording.gid AS recording_gid, work.gid, work_name.name,
			array_agg((artist.gid, artist_name.name, get_link_phrase(l_artist_work.link))::credit) AS credits
		FROM do_release_link
		JOIN release ON release.id = do_release_link.id
		JOIN medium ON medium.release = release.id
		JOIN tracklist ON tracklist.id = medium.tracklist
		JOIN track ON track.tracklist = tracklist.id
		JOIN recording ON recording.id = track.recording
		LEFT JOIN l_recording_work ON l_recording_work.entity0 = recording.id
		LEFT JOIN work ON work.id = l_recording_work.entity1
		LEFT JOIN work_name ON work_name.id = work.name
		LEFT JOIN l_artist_work ON l_artist_work.entity1 = work.id
		LEFT JOIN artist ON artist.id = l_artist_work.entity0
		LEFT JOIN artist_name ON artist_name.id = artist.name
		WHERE do_release_link.url = ''http://www.discogs.com/release/''||:release_id
		GROUP BY release.id, medium.position, track.position, recording.id, work.gid, work_name.name
		ORDER BY medium.position, track.position
	') AS t1(release_gid uuid, recording_gid uuid, gid uuid, name character varying, credits credit[])
), mb_recording AS (
	SELECT release_gid, row_number, gid, track_name, recording_name, mb_time(track_length) AS track_length,
		mb_time(recording_length) AS recording_length, number, credits
	FROM dblink(:dblink, '
		SELECT release.gid AS release_gid, row_number() OVER (ORDER BY medium.position, track.position), 
			recording.gid, ttn.name AS track_name, rtn.name AS recording_name, track.length AS track_length, 
			recording.length AS recording_length, track.number,
			CASE
				WHEN count(artist.gid) > 0
				THEN array_agg((artist.gid, artist_name.name, get_link_phrase(l_artist_recording.link))::credit)
				ELSE NULL
			END AS credits
		FROM do_release_link
		JOIN release ON release.id = do_release_link.id
		JOIN medium ON medium.release = release.id
		JOIN tracklist ON tracklist.id = medium.tracklist
		JOIN track ON track.tracklist = tracklist.id
		JOIN track_name ttn ON ttn.id = track.name
		JOIN recording ON recording.id = track.recording
		JOIN track_name rtn ON rtn.id = recording.name
		LEFT JOIN l_artist_recording ON l_artist_recording.entity1 = recording.id
		LEFT JOIN artist ON artist.id = l_artist_recording.entity0
		LEFT JOIN artist_name ON artist_name.id = artist.name
		WHERE do_release_link.url = ''http://www.discogs.com/release/''||:release_id
		GROUP BY release.id, medium.position, track.position, recording.id, ttn.name, rtn.name,
			track.length, recording.length, track.number
		ORDER BY medium.position, track.position
	') AS t1(release_gid uuid, row_number integer, gid uuid, track_name character varying, recording_name character varying, 
		track_length integer, recording_length integer, number text, credits credit[])
), mb_release AS (
	SELECT *
	FROM dblink(:dblink, '
		SELECT release.gid AS release_gid, release_name.name, release_info(release.id, ''long''),
			CASE 
				WHEN count(artist.gid) > 0
				THEN array_agg((artist.gid, artist_name.name, 
					get_link_phrase(l_artist_release.link))::credit)
				ELSE NULL
			END AS credits
		FROM do_release_link
		JOIN release ON release.id = do_release_link.id
		JOIN release_name ON release_name.id = release.name
		LEFT JOIN l_artist_release ON l_artist_release.entity1 = release.id
		LEFT JOIN artist ON artist.id = l_artist_release.entity0
		LEFT JOIN artist_name ON artist_name.id = artist.name
		WHERE do_release_link.url = ''http://www.discogs.com/release/''||:release_id
		GROUP BY release.id, release_name.name
	') AS t1(release_gid uuid, name character varying, release_info text, credits credit[])
), mb_release_artist AS (
	SELECT *
	FROM dblink(:dblink, '
		SELECT release.gid AS release_gid, recording.gid AS recording_gid, 
			array_agg((ra.gid, ran.name, NULL)::credit) AS release_credits, 
			CASE
				WHEN recording.artist_credit <> release.artist_credit
				THEN array_agg((artist.gid, artist_name.name, NULL)::credit)
				ELSE NULL
			END AS recording_credits,
			CASE
				WHEN track.artist_credit <> release.artist_credit
				THEN array_agg((ta.gid, tan.name, NULL)::credit) 
				ELSE NULL
			END AS track_credits
		FROM do_release_link
		JOIN release ON release.id = do_release_link.id
		JOIN artist_credit rac ON rac.id = release.artist_credit
		JOIN artist_credit_name racn ON racn.artist_credit = rac.id
		JOIN artist ra ON ra.id = racn.artist
		JOIN artist_name ran ON ran.id = racn.name
		JOIN medium ON medium.release = release.id
		JOIN tracklist ON tracklist.id = medium.tracklist
		JOIN track ON track.tracklist = tracklist.id
		JOIN artist_credit tac ON tac.id = track.artist_credit
		JOIN artist_credit_name tacn ON tacn.artist_credit = tac.id
		JOIN artist ta ON ta.id = tacn.artist
		JOIN artist_name tan ON tan.id = ta.name
		JOIN recording ON recording.id = track.recording
		JOIN artist_credit ON artist_credit.id = recording.artist_credit
		JOIN artist_credit_name ON artist_credit_name.artist_credit = artist_credit.id
		JOIN artist ON artist.id = artist_credit_name.artist
		JOIN artist_name ON artist_name.id = artist_credit_name.name
		WHERE do_release_link.url = ''http://www.discogs.com/release/''||:release_id
		GROUP BY release.id, medium.position, track.position, recording.id,
			release.artist_credit, track.artist_credit, recording.artist_credit
		ORDER BY medium.position, track.position
	') AS t1(release_gid uuid, recording_gid uuid, release_credits credit[], recording_credits credit[], track_credits credit[])
), do_release AS (
	SELECT *
	FROM release
	WHERE release.id = :release_id
), do_track AS (
	SELECT row_number() OVER (ORDER BY track.id), track.*
	FROM do_release
	JOIN track ON track.release_id = do_release.id
	WHERE position <> ''
	ORDER BY track.id
), do_release_credit AS (
	SELECT DISTINCT do_release.id, gid, artist_name||
		CASE
			WHEN anv ISNULL OR anv = '' THEN ''
			ELSE ' credited as '||anv
		END 
		AS artist_name,
		COALESCE(role_name, '')||
		CASE
			WHEN role_details ISNULL THEN ''
			ELSE ' ['||role_details||']'
		END 
		AS role_name, parse_tracks(tracks, (SELECT array_agg(position) FROM do_track GROUP BY release_id)) AS tracks
	FROM do_release
	JOIN releases_extraartists ON releases_extraartists.release_id = do_release.id
	LEFT JOIN mb_artist_link ON mb_artist_link.id = artist_id
), do_release_credits AS (
	SELECT id, array_agg((gid, artist_name, role_name, tracks)::do_credit) as credits
	FROM do_release_credit
	WHERE tracks = ''
	GROUP BY id
), do_track_credit AS (
	SELECT do_release.id AS release_id, track.id AS track_id,
		CASE
			WHEN count(gid) = 0 THEN NULL
			ELSE
				array_agg((gid, artist_name||
				CASE
					WHEN anv ISNULL OR anv = '' THEN ''
					ELSE ' credited as '||anv
				END,
				COALESCE(role_name, '')||
				CASE
					WHEN role_details ISNULL THEN ''
					ELSE ' ['||role_details||']'
				END, 
				track.position)::do_credit)
		END
		AS credit
	FROM do_release
	JOIN track ON track.release_id = do_release.id
	JOIN tracks_extraartists ON tracks_extraartists.track_id = track.id
	LEFT JOIN mb_artist_link ON mb_artist_link.id = artist_id
	GROUP BY do_release.id, track.id
	ORDER BY track.id
), do_track_credit2 AS (
	SELECT do_track.release_id, do_track.id AS track_id, 
		CASE
			WHEN count(do_release_credit.gid) = 0 THEN NULL
			ELSE
				array_agg((do_release_credit.gid, 
				do_release_credit.artist_name, do_release_credit.role_name, 
				do_release_credit.tracks)::do_credit) 
		END AS credit
	FROM do_track
	LEFT JOIN do_release_credit ON do_release_credit.tracks = do_track.position
	GROUP BY do_track.release_id, do_track.id
	ORDER BY do_track.id
), do_track_credits AS (
	SELECT do_track.release_id, do_track.id AS track_id, 
		CASE
			WHEN do_track_credit2.credit ISNULL
			THEN do_track_credit.credit
			WHEN do_track_credit.credit ISNULL
			THEN do_track_credit2.credit
			ELSE do_track_credit.credit||do_track_credit2.credit
		END AS credits
	FROM do_track
	LEFT JOIN do_track_credit ON do_track_credit.track_id = do_track.id
	LEFT JOIN do_track_credit2 ON do_track_credit2.track_id = do_track.id
	ORDER BY do_track.id
), tracks AS (
	SELECT do_track.release_id, mb_recording.release_gid, do_track.row_number, do_track.id, mb_recording.gid, mb_work.gid AS work_gid, 
		format_track(do_track.position, do_track.title, do_track.duration, 'Discogs track')
		||
		CASE
			WHEN do_track_credits.credits NOTNULL 
			THEN format_do_credits(do_track_credits.credits, 'recording')
			ELSE ''
		END
		||
		CASE
			WHEN mb_recording.release_gid NOTNULL 
			THEN
				CASE
					WHEN mb_recording.track_name <> mb_recording.recording_name
						OR mb_recording.track_length <> mb_recording.recording_length
					THEN format_track(mb_recording.number, mb_recording.track_name,
						mb_recording.track_length, 'MB track')
					ELSE ''
				END
				||format_track(mb_recording.number, mb_recording.recording_name, 
					mb_recording.recording_length, 'http://musicbrainz.org/recording/'||mb_recording.gid)
				||
				CASE
					WHEN mb_recording.credits NOTNULL
					THEN format_credits(mb_recording.credits, 'recording')
					ELSE ''
				END
				||
				CASE
					WHEN mb_work.gid NOTNULL
					THEN format_work(mb_work.name, 'http://musicbrainz.org/work/'||mb_work.gid)
						||format_credits(mb_work.credits, 'work')
					ELSE ''
				END
			ELSE ''
		END
		AS credit
	FROM do_track
	LEFT JOIN mb_recording ON mb_recording.row_number = do_track.row_number
	LEFT JOIN mb_work ON mb_work.recording_gid = mb_recording.gid
	LEFT JOIN do_track_credits ON do_track_credits.track_id = do_track.id
	ORDER BY do_track.row_number
), releases AS (
	SELECT do_release.id AS release_id, 
		format_release(do_release.title, 'http://www.discogs.com/release/'||do_release.id)
		||
		CASE
			WHEN do_release_credits.credits ISNULL THEN ''
			ELSE format_do_credits(do_release_credits.credits, 'release')
		END
		||
		CASE
			WHEN mb_release.release_gid NOTNULL THEN
				format_release(mb_release.name, 'http://musicbrainz.org/release/'||mb_release.release_gid)
				||
				CASE
					WHEN mb_release.credits NOTNULL THEN
						format_credits(mb_release.credits, 'release')
					ELSE ''
				END
			ELSE ''
		END
		||string_agg(tracks.credit, '')
		AS note
	FROM do_release
	LEFT JOIN do_release_credits ON do_release_credits.id = do_release.id
	LEFT JOIN mb_release_link ON mb_release_link.id = do_release.id
	LEFT JOIN mb_release ON mb_release.release_gid = mb_release_link.gid
	JOIN tracks ON tracks.release_id = do_release.id
	GROUP BY do_release.id, do_release.title, mb_release.release_gid, mb_release.name, 
		mb_release.credits, do_release_credits.credits
), track_structure AS (
	SELECT do_track.release_id, mb_recording.release_gid, do_track.row_number, do_track.id, mb_recording.gid, mb_work.gid AS work_gid, 
		format_track(do_track.position, do_track.title, do_track.duration, 'Discogs track')
		||
		CASE
			WHEN mb_recording.release_gid NOTNULL 
			THEN
				CASE
					WHEN mb_recording.track_name <> mb_recording.recording_name
						OR mb_recording.track_length <> mb_recording.recording_length
					THEN format_track(mb_recording.number, mb_recording.track_name,
						mb_recording.track_length, 'MB track')
					ELSE ''
				END
				||format_track(mb_recording.number, mb_recording.recording_name, 
					mb_recording.recording_length, 'http://musicbrainz.org/recording/'||mb_recording.gid)
				||
				CASE
					WHEN mb_work.gid NOTNULL
					THEN format_work(mb_work.name, 'http://musicbrainz.org/work/'||mb_work.gid)
					ELSE ''
				END
			ELSE ''
		END
		AS note
	FROM do_track
	LEFT JOIN mb_recording ON mb_recording.row_number = do_track.row_number
	LEFT JOIN mb_work ON mb_work.recording_gid = mb_recording.gid
	ORDER BY do_track.row_number
), release_structure AS (
	SELECT do_release.id AS release_id, mb_release.release_gid, format_release(do_release.title, 'Discogs release')||
		CASE
			WHEN mb_release.release_gid NOTNULL THEN
				format_release(mb_release.name, 'MB release')
			ELSE ''
		END
		||string_agg(track_structure.note, '')
		AS note
	FROM do_release
	LEFT JOIN mb_release_link ON mb_release_link.id = do_release.id
	LEFT JOIN mb_release ON mb_release.release_gid = mb_release_link.gid
	JOIN track_structure ON track_structure.release_id = do_release.id
	GROUP BY do_release.id, do_release.title, mb_release.release_gid, mb_release.name
), do_label AS (
	SELECT DISTINCT do_release.id AS release_id, label.id, label.name
	FROM do_release
	LEFT JOIN releases_labels ON releases_labels.release_id = do_release.id
	LEFT JOIN label ON label.id = releases_labels.label_id
), label_check1 AS (
	SELECT DISTINCT release_id, format_release('  '||do_label.name, 'http://www.discogs.com/label/'||do_label.id)||
		CASE
			WHEN count(mb_label_link.gid) = 0 THEN '  Error: MusicBrainz label not found!'||E'\r\n'
			WHEN count(mb_label_link.gid) > 1 THEN string_agg(format_release('  '||mb_label_link.name, 
				'http://musicbrainz.org/release/'||mb_label_link.gid), '')
				||'  Error: More than one MusicBrainz label links to same label!'||E'\r\n'
			ELSE string_agg(format_release('  '||mb_label_link.name, 
				'http://musicbrainz.org/release/'||mb_label_link.gid), '')
		END
		AS note,
		CASE
			WHEN count(mb_label_link.gid) = 1 THEN TRUE
			ELSE FALSE
		END
		AS status
	FROM do_label
	LEFT JOIN mb_label_link ON mb_label_link.id = do_label.id
	GROUP BY release_id, do_label.id, do_label.name
), label_check AS (
	SELECT release_id, 'Labels: '||E'\r\n'||string_agg(note, E'\r\n')||E'\r\n' AS note, bool_and(status) AS status
	FROM label_check1
	GROUP BY release_id
), master_check AS (
	SELECT do_release.id AS release_id,
		CASE
			WHEN master.id NOTNULL THEN 'Master / Release Group: '||E'\r\n'
				||format_release('  '||master.title, 'http://www.discogs.com/master/'||master.id)
				||
				CASE
					WHEN count(mb_release_group_link.gid) = 0 THEN
						'  Warning: MusicBrainz release group not found!'||E'\r\n'
					WHEN count(mb_release_group_link.gid) > 1 THEN 
						string_agg(format_release('  '||mb_release_group_link.name,
						'http://musicbrainz.org/release-group/'||mb_release_group_link.gid), '')
						||'  Error: More than one MusicBrainz release group links to this master!'||E'\r\n'
					ELSE string_agg(format_release('  '||mb_release_group_link.name,
						'http://musicbrainz.org/release-group/'||mb_release_group_link.gid), '')
				END
				||E'\r\n'
			ELSE ''
		END
		AS note,
		CASE
			WHEN master.id ISNULL OR count(mb_release_group_link.gid) >= 1 THEN TRUE
			ELSE FALSE
		END
		AS status
	FROM do_release
	LEFT JOIN master ON master.id = do_release.master_id
	LEFT JOIN mb_release_group_link ON mb_release_group_link.id = master.id
	GROUP BY do_release.id, master.id, master.title
), release_check AS (
	SELECT do_release.id AS release_id, 'Release: '||E'\r\n'
		||format_release('  '||do_release.title, 'http://www.discogs.com/release/'||do_release.id)
		||
		CASE
			WHEN count(mb_release_link.gid) > 0 THEN string_agg(format_release(
				'  '||mb_release_link.name, 'http://musicbrainz.org/release/'||mb_release_link.gid), '')
			ELSE ''
		END
		||
		CASE
			WHEN count(mb_release_link.gid) = 0 THEN '  Warning: MusicBrainz release not found!'
			WHEN count(mb_release_link.gid) > 1 THEN '  Error: More than one MusicBrainz release links to this release!'
			ELSE ''
		END
		||E'\r\n\r\n' AS note,
		CASE
			WHEN count(mb_release_link.gid) <= 1 THEN TRUE
			ELSE FALSE
		END
		AS status
	FROM do_release
	LEFT JOIN mb_release_link ON mb_release_link.id = do_release.id
	GROUP BY do_release.id, do_release.title
), do_artist AS (
	WITH all_artists AS (
		SELECT artist_name, artist_id
		FROM do_track
		JOIN tracks_extraartists ON tracks_extraartists.track_id = do_track.id
		UNION
		SELECT artist_name, artist_id
		FROM do_track
		JOIN tracks_artists ON tracks_artists.track_id = do_track.id
		UNION
		SELECT artist_name, artist_id
		FROM do_release
		JOIN releases_artists ON releases_artists.release_id = do_release.id
		UNION
		SELECT artist_name, artist_id
		FROM do_release
		JOIN releases_extraartists ON releases_extraartists.release_id = do_release.id
	)
	SELECT artist.*
	FROM all_artists
	JOIN artist ON artist.id = all_artists.artist_id
), artist_check1 AS (
	SELECT (SELECT id FROM do_release) AS release_id, format_release('  '||do_artist.name, 'http://www.discogs.com/artist/'||do_artist.id)
		||
		CASE
			WHEN count(mb_artist_link.gid) > 0 THEN string_agg(format_release(
				'  '||mb_artist_link.name, 'http://musicbrainz.org/artist/'||mb_artist_link.gid), '')
			ELSE ''
		END
		||
		CASE
			WHEN count(mb_artist_link.gid) = 0 THEN '  Error: No MusicBrainz artist found!'||E'\r\n'
			WHEN count(mb_artist_link.gid) > 1 THEN '  Error: More than one MusicBrainz artist found!'||E'\r\n'
			ELSE ''
		END
		||E'\r\n' AS note,
		CASE
			WHEN count(mb_artist_link.gid) = 1 THEN TRUE
			ELSE FALSE
		END
		AS status
	FROM do_artist
	LEFT JOIN mb_artist_link ON mb_artist_link.id = do_artist.id
	GROUP BY do_artist.id, do_artist.name
	ORDER BY do_artist.name
), artist_check AS (
	SELECT release_id, 'Artists: '||E'\r\n'||string_agg(note, '') AS note, bool_and(status) AS status
	FROM artist_check1
	GROUP BY release_id
), status AS (
	SELECT release_check.release_id,
		release_check.note||E'\r\n'||master_check.note||E'\r\n'||label_check.note||E'\r\n'||artist_check.note||E'\r\n' AS note,
		release_check.status AND master_check.status AND label_check.status AND artist_check.status AS status
	FROM release_check
	LEFT JOIN master_check ON master_check.release_id = release_check.release_id
	LEFT JOIN label_check ON label_check.release_id = release_check.release_id
	LEFT JOIN artist_check ON artist_check.release_id = release_check.release_id
)
SELECT status.note||E'\r\n'||COALESCE(mb_release.release_info, '')||E'\r\n'
	||E'\r\n'||release_info(status.release_id, 'long')||E'\r\n'
	||E'\r\n'||'Tracks / Recordings and works:'||E'\r\n'||release_structure.note AS note
FROM status
LEFT JOIN release_structure ON release_structure.release_id = status.release_id
LEFT JOIN releases ON releases.release_id = status.release_id
LEFT JOIN mb_release ON mb_release.release_gid = release_structure.release_gid;
