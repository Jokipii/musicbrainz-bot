CREATE OR REPLACE FUNCTION decodeUrl(url text, remove text)
  RETURNS text
AS $$
  import urllib.parse
  global url, remove
  result = urllib.parse.unquote_plus(url)
  return result[len(remove):]
$$ LANGUAGE plpython3u;

CREATE OR REPLACE FUNCTION url(name text)
  RETURNS text
AS $$
  import urllib.parse
  global name
  encoded = urllib.parse.quote_plus(name,'()')
  return encoded
$$ LANGUAGE plpython3u;

CREATE OR REPLACE FUNCTION explode(in_array anyarray) RETURNS SETOF anyelement AS
$$
    SELECT ($1)[s] FROM generate_series(1,array_upper($1, 1)) AS s;
$$
LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION release_info(releaseid integer, outputtype text) RETURNS text AS $$
DECLARE
	result text;
BEGIN
	WITH do_release AS (
		SELECT *
		FROM release
		WHERE release.id = releaseid
	), labels AS (
		SELECT release_id, string_agg(
			label||' - '||catno
			,
			CASE
				WHEN outputtype = 'short' THEN ', '
				WHEN outputtype = 'long' THEN E'\r\n  '
			END
			) AS label
		FROM do_release
		JOIN releases_labels ON releases_labels.release_id = do_release.id
		GROUP BY release_id
	), formats AS ( 
		SELECT release_id, string_agg(
			CASE
				WHEN qty = 1 THEN ''
				ELSE qty::text||E' \u00D7 '
			END
			||format_name||
			CASE
				WHEN array_length(descriptions, 1) = 0 THEN ''
				ELSE ', '||array_to_string(descriptions, ', ') 
			END
			||
			CASE
				WHEN (text = '' OR text ISNULL) THEN ''
				ELSE ', '||text
			END 
			,
			CASE
				WHEN outputtype = 'short' THEN ', '
				WHEN outputtype = 'long' THEN E'\r\n    '
			END
			) AS format
		FROM do_release
		JOIN releases_formats ON releases_formats.release_id = do_release.id
		GROUP BY release_id
	), identifiers AS (
		WITH identifier AS (
			SELECT release_id, type, type||': '||string_agg(
				value||
				CASE
					WHEN (description = '' OR description ISNULL) THEN ''
					ELSE ' ('||description||')'
				END
				,
				E'\r\n    '
				) AS identifier
			FROM do_release
			JOIN release_identifier ON release_identifier.release_id = do_release.id
			WHERE type <> 'Barcode'
			GROUP BY release_id, type
		)
		SELECT release_id, string_agg(identifier, E'\r\n  ') AS identifier
		FROM identifier
		GROUP BY release_id
	), barcodes AS (
		WITH barcode AS (
			SELECT DISTINCT release_id, 
				CASE
					WHEN outputtype = 'short' THEN value
					WHEN outputtype = 'long' THEN 
						value||
						CASE
							WHEN (description = '' OR description ISNULL) THEN ''
							ELSE ' ('||description||')'
						END
				END
				AS barcode
			FROM do_release
			JOIN release_identifier ON release_identifier.release_id = do_release.id
			WHERE type = 'Barcode'
		)
		SELECT release_id,
			CASE
				WHEN outputtype = 'long' AND count(barcode) > 2 THEN string_agg(barcode, E'\r\n    ')
				ELSE string_agg(barcode, ', ')
			END
			AS barcode
		FROM barcode
		GROUP BY release_id
	)
	SELECT
		CASE
			WHEN outputtype = 'short' THEN
				'Format: '||COALESCE(formats.format, '')||', '
				||'Country: '||COALESCE(country, '')||', '
				||'Date: '||COALESCE(released, '')||', '
				||'Tracks: '
				||(SELECT count(*)::text
					FROM do_release
					JOIN track ON track.release_id = do_release.id
					WHERE position <> ''
					GROUP BY release_id)
				||', '
				||'Barcode: '||COALESCE(barcodes.barcode, '')||', '
				||'Label: '||COALESCE(labels.label, '')
			WHEN outputtype = 'long' THEN
				'Release information'||E'\r\n'
				||'  Released: '||COALESCE(released, '')||E'\r\n' 
				||'  Country: '||COALESCE(country, '')||E'\r\n'
				||'  Barcode: '||COALESCE(barcodes.barcode, '')||E'\r\n'
				||'  Format: '||COALESCE(formats.format, '')||E'\r\n'
				||E'\r\n'||'Additional details'||E'\r\n'
				||'  Genre: '||array_to_string(genres, ', ')||E'\r\n'
				||'  Style: '||array_to_string(styles, ', ')||E'\r\n'
				||'  Data Quality: '||data_quality||E'\r\n'
				||E'\r\n'||'Labels'||E'\r\n'||'  '||COALESCE(labels.label, '')||E'\r\n'
				||
				CASE
					WHEN notes ISNULL THEN ''
					ELSE E'\r\n'||'Notes'||E'\r\n'||notes||E'\r\n'
				END
				||
				CASE
					WHEN identifiers.identifier ISNULL THEN ''
					ELSE E'\r\n'||'Other identifiers'||E'\r\n'||'  '||identifiers.identifier||E'\r\n'
				END
			ELSE
				'Type not known'
		END
		AS text
	FROM do_release
	INTO result
	LEFT JOIN labels ON labels.release_id = do_release.id
	LEFT JOIN formats ON formats.release_id = do_release.id
	LEFT JOIN barcodes ON barcodes.release_id = do_release.id
	LEFT JOIN identifiers ON identifiers.release_id = do_release.id;

	RETURN result;
END
$$ LANGUAGE 'plpgsql';
