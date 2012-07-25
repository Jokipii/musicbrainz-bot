DROP INDEX IF EXISTS l_release_url_idx_discogs, l_release_group_url_idx_discogs, l_label_url_idx_discogs, l_artist_url_idx_discogs;

CREATE INDEX l_release_url_idx_discogs ON l_release_url(link) WHERE link = 6301;
CREATE INDEX l_release_group_url_idx_discogs ON l_release_group_url(link) WHERE link = 6309;
CREATE INDEX l_label_url_idx_discogs ON l_label_url(link) WHERE link = 27037;
CREATE INDEX l_artist_url_idx_discogs ON l_artist_url(link) WHERE link = 26038;

CREATE OR REPLACE VIEW do_release_link AS
SELECT release.id, release_name.name, release.gid, url.url, l_release_url.edits_pending
FROM l_release_url
JOIN url ON l_release_url.entity1 = url.id
JOIN release ON l_release_url.entity0 = release.id
JOIN release_name ON release.name = release_name.id
WHERE l_release_url.link = 6301;

CREATE OR REPLACE VIEW do_release_group_link AS
SELECT release_group.id, release_name.name, release_group.gid, url.url, l_release_group_url.edits_pending
FROM l_release_group_url
JOIN url ON l_release_group_url.entity1 = url.id
JOIN release_group ON l_release_group_url.entity0 = release_group.id
JOIN release_name ON release_group.name = release_name.id
WHERE l_release_group_url.link = 6309;

CREATE OR REPLACE VIEW do_label_link AS
SELECT label.id, label_name.name, label.gid, url.url, l_label_url.edits_pending
FROM l_label_url
JOIN url ON l_label_url.entity1 = url.id
JOIN label ON l_label_url.entity0 = label.id
JOIN label_name ON label.name = label_name.id
WHERE l_label_url.link = 27037;

CREATE OR REPLACE VIEW do_artist_link AS
SELECT artist.id, artist_name.name, artist.gid, url.url, l_artist_url.edits_pending
FROM l_artist_url
JOIN url ON l_artist_url.entity1 = url.id
JOIN artist ON l_artist_url.entity0 = artist.id
JOIN artist_name ON artist.name = artist_name.id
WHERE l_artist_url.link = 26038;

DROP TABLE IF EXISTS matviews;

CREATE TABLE matviews (
	mv_name NAME NOT NULL PRIMARY KEY,
	v_name NAME NOT NULL,
	last_refresh TIMESTAMP WITH TIME ZONE
);

CREATE OR REPLACE FUNCTION create_matview(NAME, NAME)
RETURNS VOID
SECURITY DEFINER
LANGUAGE plpgsql AS '
DECLARE
	matview ALIAS FOR $1;
	view_name ALIAS FOR $2;
	entry matviews%ROWTYPE;
BEGIN
	SELECT * INTO entry FROM matviews WHERE mv_name = matview;
	IF FOUND THEN
		RAISE EXCEPTION ''Materialized view ''''%'''' already exists.'', matview;
	END IF;
	EXECUTE ''REVOKE ALL ON '' || view_name || '' FROM PUBLIC''; 
	EXECUTE ''GRANT SELECT ON '' || view_name || '' TO PUBLIC'';
	EXECUTE ''CREATE TABLE '' || matview || '' AS SELECT * FROM '' || view_name;
	EXECUTE ''REVOKE ALL ON '' || matview || '' FROM PUBLIC'';
	EXECUTE ''GRANT SELECT ON '' || matview || '' TO PUBLIC'';
	INSERT INTO matviews (mv_name, v_name, last_refresh)
		VALUES (matview, view_name, CURRENT_TIMESTAMP); 
	RETURN;
END
';

CREATE OR REPLACE FUNCTION drop_matview(NAME) RETURNS VOID
SECURITY DEFINER
LANGUAGE plpgsql AS '
DECLARE
	matview ALIAS FOR $1;
	entry matviews%ROWTYPE;
BEGIN
	SELECT * INTO entry FROM matviews WHERE mv_name = matview;
	IF NOT FOUND THEN
		RAISE EXCEPTION ''Materialized view % does not exist.'', matview;
	END IF;
	EXECUTE ''DROP TABLE '' || matview;
	DELETE FROM matviews WHERE mv_name=matview;
	RETURN;
END
';

CREATE OR REPLACE FUNCTION refresh_matview(name) RETURNS VOID
SECURITY DEFINER
LANGUAGE plpgsql AS '
DECLARE 
	matview ALIAS FOR $1;
	entry matviews%ROWTYPE;
BEGIN
	SELECT * INTO entry FROM matviews WHERE mv_name = matview;
	IF NOT FOUND THEN
		RAISE EXCEPTION ''Materialized view % does not exist.'', matview;
	END IF;
	EXECUTE ''DELETE FROM '' || matview;
	EXECUTE ''INSERT INTO '' || matview|| '' SELECT * FROM '' || entry.v_name;
	UPDATE matviews
		SET last_refresh=CURRENT_TIMESTAMP
		WHERE mv_name=matview;
	RETURN;
END
';

ALTER VIEW do_release_link RENAME TO do_release_link_view;
SELECT create_matview('do_release_link', 'do_release_link_view');
CREATE INDEX do_release_link_idx_gid ON do_release_link(gid);
CREATE INDEX do_release_link_idx_url ON do_release_link(url);
ANALYZE do_release_link;

ALTER VIEW do_artist_link RENAME TO do_artist_link_view;
SELECT create_matview('do_artist_link', 'do_artist_link_view');
CREATE INDEX do_artist_link_idx_gid ON do_artist_link(gid);
CREATE INDEX do_artist_link_idx_url ON do_artist_link(url);
ANALYZE do_artist_link;

CREATE OR REPLACE FUNCTION get_link_phrase(link_id integer) RETURNS text AS $$
DECLARE
	result text;
	att RECORD;
BEGIN
	SELECT link_type.link_phrase::text
	INTO result
	FROM link
	JOIN link_type ON link_type.id = link.link_type
	WHERE link.id = link_id;
	
	FOR att IN
		SELECT root.name AS root, COALESCE(string_agg(value.name, ', '), ''::text) AS value
		FROM link
		JOIN link_type ON link_type.id = link.link_type
		JOIN link_type_attribute_type ON link_type_attribute_type.link_type = link_type.id
		JOIN link_attribute_type root ON root.id = link_type_attribute_type.attribute_type
		LEFT JOIN link_attribute ON link_attribute.link = link.id
		LEFT JOIN link_attribute_type value ON (value.id = link_attribute.attribute_type AND value.root = root.id)
		WHERE link.id = link_id
		GROUP BY link.id, root.name
	LOOP
		result = regexp_replace(result, '{'||att.root||'(:.+?)?}', att.value);
	END LOOP;
	
	RETURN trim(both FROM replace(result, '  ', ' '));
END
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION release_info(releaseid integer, outputtype text) RETURNS text AS $$
DECLARE
	result text;
	indent text;
BEGIN
	CASE
		WHEN outputtype = 'short' THEN indent := '';
		ELSE indent := '  ';
	END CASE;
	WITH rel AS (
		SELECT release.*
		FROM release
		WHERE release.id = releaseid
	), m_format AS (
		SELECT *
		FROM medium_format
	), rg_primary_type AS (
		SELECT *
		FROM release_group_primary_type
	), rg_secondary_type AS (
		SELECT *
		FROM release_group_secondary_type
	), r_status AS (
		SELECT *
		FROM release_status
	), r_packaging AS (
		SELECT *
		FROM release_packaging
	), medi AS (
		SELECT medium.release AS release_id, medium.id AS medium_id, tracklist, format
		FROM medium
		WHERE medium.release = releaseid
	), labels AS (
		WITH labels AS (
			SELECT release_label.release AS id, label_name.name, release_label.catalog_number
			FROM release_label
			JOIN label ON label.id = release_label.label
			JOIN label_name ON label_name.id = label.name
			WHERE release_label.release = releaseid
		)
		SELECT id, string_agg(
			name||' - '||catalog_number
			,
			CASE
				WHEN outputtype = 'short' THEN ', '
				WHEN outputtype = 'long' THEN E'\r\n  '
			END
			) AS note
		FROM labels
		GROUP BY id
	), format AS (
		WITH forc AS (
			SELECT release_id AS id, count(medium_id), (array_agg(m_format.name))[1] AS name
			FROM medi
			JOIN m_format ON m_format.id = medi.format
			GROUP BY release_id, m_format.id
		)
		SELECT id, string_agg(
			CASE
				WHEN count = 1 THEN name
				ELSE count||E' \u00D7 '||name
			END, ', ') AS format
		FROM forc
		GROUP BY id
	), date AS (
		SELECT COALESCE(rel.date_year::text, '')|| 
			CASE
				WHEN rel.date_month ISNULL THEN ''
				ELSE '-'||lpad(rel.date_month::text, 2, '0')||
					CASE
						WHEN rel.date_day ISNULL THEN ''
						ELSE '-'||lpad(rel.date_day::text, 2, '0')
					END
			END
			AS date
		FROM rel
	), type AS (
		SELECT COALESCE(rg_primary_type.name, '')||
			CASE
				WHEN count(rg_secondary_type.name) = 0 THEN ''
				ELSE ' + '||string_agg(rg_secondary_type.name, ', ')
			END
			AS type
		FROM rel
		JOIN release_group ON release_group.id = rel.release_group
		JOIN rg_primary_type ON rg_primary_type.id = release_group.type
		LEFT JOIN release_group_secondary_type_join ON release_group_secondary_type_join.release_group = release_group.id
		LEFT JOIN rg_secondary_type ON rg_secondary_type.id = release_group_secondary_type_join.secondary_type
		GROUP BY rg_primary_type.name
	), add_info AS (
		SELECT rel.id, array[
			CASE
				WHEN (SELECT type FROM type) ISNULL THEN NULL
				ELSE indent||'Type: '||(SELECT type FROM type)
			END,
			CASE
				WHEN r_packaging.name ISNULL THEN NULL
				ELSE indent||'Packaging: '||r_packaging.name
			END,
			CASE
				WHEN r_status.name ISNULL THEN NULL
				ELSE indent||'Status: '||r_status.name
			END
			]::text[] AS info
		FROM rel
		LEFT JOIN r_packaging ON r_packaging.id = rel.packaging
		LEFT JOIN r_status ON r_status.id = rel.status
	), short_info AS (
		WITH track_count AS (
			SELECT sum(track_count)::text
			FROM medi
			JOIN tracklist ON tracklist.id = medi.tracklist
			GROUP BY release_id
		)
		SELECT rel.id, array[
			CASE
				WHEN format.format ISNULL THEN NULL
				ELSE 'Format: '||format.format
			END,
			CASE
				WHEN country.iso_code ISNULL THEN NULL
				ELSE 'Country: '||country.iso_code
			END,
			CASE
				WHEN (SELECT date FROM date) ISNULL THEN NULL
				ELSE 'Date: '||(SELECT date FROM date)
			END,
			CASE
				WHEN (SELECT sum FROM track_count) ISNULL THEN NULL
				ELSE 'Tracks: '||(SELECT sum FROM track_count)
			END,
			CASE
				WHEN rel.barcode ISNULL THEN NULL
				ELSE 'Barcode: '||rel.barcode
			END,
			CASE
				WHEN labels.note ISNULL THEN NULL
				ELSE 'Label: '||labels.note
			END
			]::text[]||(SELECT info FROM add_info) AS info
		FROM rel
		LEFT JOIN labels ON labels.id = releaseid
		LEFT JOIN format ON format.id = releaseid
		LEFT JOIN country ON country.id = rel.country
	), long_info AS (
		SELECT rel.id, array[
			'Release information',
			CASE
				WHEN (SELECT date FROM date) ISNULL THEN NULL
				ELSE '  Date: '||(SELECT date FROM date)
			END,
			CASE
				WHEN country.iso_code ISNULL THEN NULL
				ELSE '  Country: '||country.iso_code
			END,
			CASE
				WHEN rel.barcode ISNULL THEN NULL
				ELSE '  Barcode: '||rel.barcode
			END,
			CASE
				WHEN format.format ISNULL THEN NULL
				ELSE '  Format: '||format.format
			END,
			''::text,
			'Additional details'
			]::text[]
			||(SELECT info FROM add_info)||array[
			CASE
				WHEN language.name ISNULL THEN NULL
				ELSE '  Language: '||language.name
			END,
			CASE
				WHEN script.name ISNULL THEN NULL
				ELSE '  Script: '||script.name
			END,
			'  Data Quality: '||
			CASE
				WHEN rel.quality = -1 OR rel.quality = 1 THEN 'Normal'
				WHEN rel.quality = 0 THEN 'Low'
				ELSE 'High'
			END,
			''::text,
			'Labels',
			COALESCE(labels.note, '')
			]::text[] AS info
		FROM rel
		LEFT JOIN labels ON labels.id = rel.id
		LEFT JOIN format ON format.id = rel.id
		LEFT JOIN country ON country.id = rel.country
		LEFT JOIN script ON script.id = rel.script
		LEFT JOIN language ON language.id = rel.language
	)
	SELECT 
		CASE
			WHEN outputtype = 'short' THEN
				(SELECT array_to_string(info, ', ') FROM short_info)
			WHEN outputtype = 'long' THEN
				(SELECT array_to_string(info, E'\r\n') FROM long_info)
		END
		AS info
	INTO result;

	RETURN result;
END
$$ LANGUAGE 'plpgsql';
