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
