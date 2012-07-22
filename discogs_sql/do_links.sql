WITH mbr AS (
	SELECT t1.gid, t1.name, t1.url, (regexp_matches(t1.url, '(?:^http://www.discogs.com/release/)([0-9]+)'))[1]::integer AS id, edits_pending
	FROM dblink(:dblink, '
		SELECT gid, name, url, edits_pending FROM do_release_link WHERE url ~ ''^http://www.discogs.com/release/''
	') AS t1(gid uuid, name text, url text, edits_pending integer)
), add1 AS (
	SELECT gid, name, url, id, edits_pending
	FROM mbr
	EXCEPT
	SELECT gid, name, url, id, edits_pending
	FROM mb_release_link
), remove AS (
	SELECT gid, name, url, id, edits_pending
	FROM mb_release_link
	EXCEPT
	SELECT gid, name, url, id, edits_pending
	FROM mbr
), add2 AS (
	INSERT INTO mb_release_link(gid, name, url, id, edits_pending) 
    SELECT gid, name, url, id, edits_pending
    FROM add1 
    WHERE add1.id IN (SELECT id FROM release)
	RETURNING *
), remove2 AS (
    DELETE FROM mb_release_link
    USING remove
    WHERE mb_release_link.gid = remove.gid
	AND mb_release_link.name = remove.name
	AND mb_release_link.url = remove.url
	AND mb_release_link.id = remove.id
	AND mb_release_link.edits_pending = remove.edits_pending
	RETURNING remove.*
)
INSERT INTO update_log(updated, event, timest)
SELECT 'mb_release_link' AS updated, 'removed: '||(SELECT count(*) FROM remove2)
	||', added: '||(SELECT count(*) FROM add2) AS event, now();

WITH mbr AS (
	SELECT t1.gid, t1.name, t1.url, (regexp_matches(t1.url, '(?:^http://www.discogs.com/master/)([0-9]+)'))[1]::integer AS id, edits_pending
	FROM dblink(:dblink, '
		SELECT gid, name, url, edits_pending FROM do_release_group_link WHERE url ~ ''^http://www.discogs.com/master/''
	') AS t1(gid uuid, name text, url text, edits_pending integer)
), add1 AS (
	SELECT gid, name, url, id, edits_pending
	FROM mbr
	EXCEPT
	SELECT gid, name, url, id, edits_pending
	FROM mb_release_group_link
), remove AS (
	SELECT gid, name, url, id, edits_pending
	FROM mb_release_group_link
	EXCEPT
	SELECT gid, name, url, id, edits_pending
	FROM mbr
), add2 AS (
	INSERT INTO mb_release_group_link(gid, name, url, id, edits_pending)
	SELECT gid, name, url, id, edits_pending
	FROM add1
	WHERE add1.id IN (SELECT id FROM master)
	RETURNING *
), remove2 AS (
	DELETE FROM mb_release_group_link 
	USING remove 
	WHERE mb_release_group_link.gid = remove.gid 
	AND mb_release_group_link.name = remove.name
	AND mb_release_group_link.url = remove.url
	AND mb_release_group_link.id = remove.id
	AND mb_release_group_link.edits_pending = remove.edits_pending
	RETURNING remove.*
)
INSERT INTO update_log(updated, event, timest)
SELECT 'mb_release_group_link' AS updated, 'removed: '||(SELECT count(*) FROM remove2)
	||', added: '||(SELECT count(*) FROM add2) AS event, now();

WITH init AS (
	SELECT t1.gid, t1.name, t1.url, lower(decodeURL(t1.url, 'http://www.discogs.com/artist/')) AS id2, t1.edits_pending
	FROM dblink(:dblink, '
		SELECT gid, name, url, edits_pending FROM do_artist_link
	') AS t1(gid uuid, name text, url text, edits_pending integer)
), mbr AS (
	SELECT init.gid, init.name, init.url, artist.id, init.edits_pending
	FROM init
	JOIN artist ON lower(artist.name) = init.id2
), add1 AS (
	SELECT gid, name, url, id, edits_pending
	FROM mbr
	EXCEPT
	SELECT gid, name, url, id, edits_pending
	FROM mb_artist_link
), remove AS (
	SELECT gid, name, url, id, edits_pending
	FROM mb_artist_link
	EXCEPT
	SELECT gid, name, url, id, edits_pending
	FROM mbr
), add2 AS (
	INSERT INTO mb_artist_link(gid, name, url, id, edits_pending)
    SELECT gid, name, url, id, edits_pending
    FROM add1
    RETURNING *
), del_new1 AS (
    DELETE FROM discogs_db_artist_evidence_release_credits
), del_new2 AS (
    DELETE FROM discogs_db_artist_evidence_track WHERE gid IN (SELECT gid FROM add1)
), del_new3 AS (
    DELETE FROM discogs_db_artist_all WHERE gid IN (SELECT gid FROM add1)
), remove2 AS (
	DELETE FROM mb_artist_link 
	USING remove 
	WHERE mb_artist_link.gid = remove.gid 
	AND mb_artist_link.name = remove.name
	AND mb_artist_link.url = remove.url
	AND mb_artist_link.id = remove.id
	AND mb_artist_link.edits_pending = remove.edits_pending
	RETURNING remove.*
)
INSERT INTO update_log(updated, event, timest)
SELECT 'mb_artist_link' AS updated, 'removed: '||(SELECT count(*) FROM remove2)
	||', added: '||(SELECT count(*) FROM add2) AS event, now();

WITH init AS (
	SELECT t1.gid, t1.name, t1.url, lower(decodeURL(t1.url, 'http://www.discogs.com/label/')) AS id2, t1.edits_pending
	FROM dblink(:dblink, '
		SELECT gid, name, url, edits_pending FROM do_label_link
	') AS t1(gid uuid, name text, url text, edits_pending integer)
), mbr AS (
	SELECT init.gid, init.name, init.url, label.id, init.edits_pending
	FROM init
	JOIN label ON lower(label.name) = init.id2
), add1 AS (
	SELECT gid, name, url, id, edits_pending
	FROM mbr
	EXCEPT
	SELECT gid, name, url, id, edits_pending
	FROM mb_label_link
), remove AS (
	SELECT gid, name, url, id, edits_pending
	FROM mb_label_link
	EXCEPT
	SELECT gid, name, url, id, edits_pending
	FROM mbr
), add2 AS (
	INSERT INTO mb_label_link(gid, name, url, id, edits_pending)
    SELECT gid, name, url, id, edits_pending
    FROM add1
    RETURNING *
), remove2 AS (
	DELETE FROM mb_label_link 
	USING remove 
	WHERE mb_label_link.gid = remove.gid 
	AND mb_label_link.name = remove.name
	AND mb_label_link.url = remove.url
	AND mb_label_link.id = remove.id
	AND mb_label_link.edits_pending = remove.edits_pending
	RETURNING remove.*
)
INSERT INTO update_log(updated, event, timest)
SELECT 'mb_label_link' AS updated, 'removed: '||(SELECT count(*) FROM remove2)
	||', added: '||(SELECT count(*) FROM add2) AS event, now();

SELECT updated, event FROM update_log ORDER BY id DESC LIMIT 4;