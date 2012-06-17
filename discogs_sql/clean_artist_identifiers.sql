INSERT INTO artist(id, name, data_quality) VALUES (0, 'Missing-afjaiahytia', 'Needs Major Changes');

WITH ids AS (
	SELECT name, min(id), max(id)
	FROM artist
	GROUP BY name
	HAVING count(id) > 1
)
DELETE FROM artist WHERE id IN (SELECT max FROM ids);

WITH ids AS (
	WITH a AS (
		SELECT DISTINCT artist_id FROM tracks_extraartists
	), b AS (
		SELECT DISTINCT id FROM artist
	)
	SELECT artist_id FROM a
	EXCEPT
	SELECT id FROM b
)
UPDATE tracks_extraartists SET artist_id = 0 
WHERE artist_id IN (SELECT artist_id FROM ids);

ALTER TABLE ONLY tracks_extraartists
ADD CONSTRAINT tracks_extraartists_artist_id_fkey FOREIGN KEY (artist_id) REFERENCES artist(id);

WITH ids AS (
	WITH a AS (
		SELECT DISTINCT artist_id FROM tracks_artists
	), b AS (
		SELECT DISTINCT id FROM artist
	)
	SELECT artist_id FROM a
	EXCEPT
	SELECT id FROM b
)
UPDATE tracks_artists SET artist_id = 0 
WHERE artist_id IN (SELECT artist_id FROM ids);

ALTER TABLE ONLY tracks_artists
ADD CONSTRAINT tracks_artists_artist_id_fkey FOREIGN KEY (artist_id) REFERENCES artist(id);

WITH ids AS (
	WITH a AS (
		SELECT DISTINCT artist_id FROM releases_artists
	), b AS (
		SELECT DISTINCT id FROM artist
	)
	SELECT artist_id FROM a
	EXCEPT
	SELECT id FROM b
)
UPDATE releases_artists SET artist_id = 0 
WHERE artist_id IN (SELECT artist_id FROM ids);

ALTER TABLE ONLY releases_artists
ADD CONSTRAINT releases_artists_artist_id_fkey FOREIGN KEY (artist_id) REFERENCES artist(id);

WITH ids AS (
	WITH a AS (
		SELECT DISTINCT artist_id FROM releases_extraartists
	), b AS (
		SELECT DISTINCT id FROM artist
	)
	SELECT artist_id FROM a
	EXCEPT
	SELECT id FROM b
)
UPDATE releases_extraartists SET artist_id = 0 
WHERE artist_id IN (SELECT artist_id FROM ids);

ALTER TABLE ONLY releases_extraartists
ADD CONSTRAINT releases_extraartists_artist_id_fkey FOREIGN KEY (artist_id) REFERENCES artist(id);

CREATE INDEX artist_lower_name_idx ON artist(lower(name));
CREATE UNIQUE INDEX artist_name_idx ON artist(name);
