import config as cfg
import sqlalchemy
from sqlalchemy.sql import text
from editing import MusicBrainzClient

class DiscogsDbClient(object):
    
    def __init__(self):
        self.mbengine = sqlalchemy.create_engine(cfg.MB_DB)
        self.doengine = sqlalchemy.create_engine(cfg.DO_DB)

    def commit_artist_links(self, limit):
        mbClient = self.open(do=True, client=True)
        queryLinks = "SELECT artist_gid, url, releases FROM discogs_db_artist_link LIMIT %s"
        queryDelete = "DELETE FROM discogs_db_artist_link WHERE artist_gid = %s"
        for gid, url, releases in self.dodb.execute(queryLinks, limit):
            note = "Exact match on name.\nArtist have at least one release (" + releases + ") that have Discogs url.\n"\
                + "All releases found that way point on same artist at Discogs."
            mbClient.add_url("artist", gid, 180, "http://www.discogs.com/artist/" + url, note)
            self.dodb.execute(queryDelete, gid)
            print url + " Done!"
        self.close()

    def commit_artist_links2(self, limit):
        mbClient = self.open(do=True, client=True)
        queryLinks = "SELECT artist_gid, url, releases FROM discogs_db_artist_link2 LIMIT %s"
        queryDelete = "DELETE FROM discogs_db_artist_link2 WHERE artist_gid = %s"
        for gid, url, releases in self.dodb.execute(queryLinks, limit):
            mbClient.add_url("artist", gid, 180, url, releases)
            self.dodb.execute(queryDelete, gid)
            print url + " Done!"
        self.close()

    def commit_release_links(self, limit):
        mbClient = self.open(do=True, client=True)
        queryLinks = "SELECT gid, url, note FROM discogs_db_release_link LIMIT %s"
        queryDelete = "DELETE FROM discogs_db_release_link WHERE gid = %s"
        for gid, url, note in self.dodb.execute(queryLinks, limit):
            mbClient.add_url("release", gid, 76, url, note)
            self.dodb.execute(queryDelete, gid)
            print url + " Done!"
        self.close()

    def commit_release_artist_relationship(self, limit):
        mbClient = self.open(do=True, client=True)
        queryLinks = "SELECT release_gid, release_url, artist_gid, artist_url FROM do_release_artist_credits LIMIT %s"
        queryDelete = "DELETE FROM do_release_artist_credits WHERE release_gid = %s AND artist_gid = %s"
        for release_gid, release_url, artist_gid, artist_url in self.mbdb.execute(queryLinks, limit):
            note = "Linked release: " + release_url + "\nLinked artist: " + artist_url
            mbClient.add_relationship("artist", "release", artist_gid, release_gid, 30, {}, note)
            self.mbdb.execute(queryDelete, release_gid, artist_gid)
            print release_gid + " " + artist_gid + " Done!"
        self.close()

    def commit_artist_types(self, limit):
        mbClient = self.open(mb=True, client=True)
        query1 = """
SELECT DISTINCT artist.gid, 'Singer/actor/producer/composer/conductor/dj in disambiguation comment' AS note
FROM artist
WHERE type ISNULL AND lower(comment) LIKE ANY(array[
'%% singer', '%% singer %%','singer %%',
'%% actor', '%% actor %%','actor %%',
'%% producer','%% producer %%','producer %%',
'%% composer','%% composer %%','composer %%',
'%% conductor','%% conductor %%','conductor %%',
'%% dj','%% dj %%','dj %%'
])
AND NOT lower(comment) LIKE ANY(array[
'%%unknown%%','%%feat%%'
])
LIMIT %s
 """
        self.commit_artist_type(query1, limit, 1, mbClient)
        query2 = """
SELECT DISTINCT artist.gid, 'Band/group/duo/duet/trio/quartet/orchestra/ensemble in disambiguation comment' AS note
FROM artist
WHERE type ISNULL AND lower(comment) LIKE ANY(array[
'%% band','%% band %%','band %%',
'%% group','%% group %%','group %%',
'%% trio','%% trio %%','trio %%',
'%% quartet','%% quartet %%','quartet %%',
'%% duo','%% duo %%','duo %%',
'%% duet','%% duet %%','duet %%',
'%% orchestra','%% orchestra %%','orchestra %%',
'%% ensemble','%% ensemble %%','ensemble %%'
])
AND NOT lower(comment) LIKE ANY(array[
'%%unknown%%','%%artist%%band%%','%%performed%%','%%feat%%','%%artist%%group%%','%%composer%%'
])
LIMIT %s
"""
        self.commit_artist_type(query2, limit, 2, mbClient)
        self.close()

    def commit_artist_type(self, query, limit, type, mbClient):
        for gid, note in self.mbdb.execute(query, limit):
            mbClient.set_artist_type(gid, type, note)
            print gid + " Done!"

    def createlinks(self):
        self.open(do=True)
        doquery = """
DROP TABLE IF EXISTS mb_release_link, mb_release_group_link, mb_artist_link, mb_label_link;

SELECT t1.gid, t1.name, t1.url, decodeURL(t1.url, 'http://www.discogs.com/release/')::integer AS id2 
INTO mb_release_link 
FROM dblink(:dblink, 'SELECT gid, name, url FROM do_release_link WHERE url LIKE ''http://www.discogs.com/release/%%''')
AS t1(gid uuid, name text, url text);
ALTER TABLE mb_release_link ADD COLUMN id integer;
UPDATE mb_release_link SET id = release.id FROM release WHERE id2 = release.id;
DELETE FROM mb_release_link WHERE id IS NULL;
ALTER TABLE mb_release_link DROP COLUMN id2;

SELECT t1.gid, t1.name, t1.url, decodeURL(t1.url, 'http://www.discogs.com/master/')::integer AS id2 
INTO mb_release_group_link 
FROM dblink(:dblink, 'SELECT gid, name, url FROM do_release_group_link WHERE url LIKE ''http://www.discogs.com/master/%%''')
AS t1(gid uuid, name text, url text);
ALTER TABLE mb_release_group_link ADD COLUMN id integer;
UPDATE mb_release_group_link SET id = master.id FROM master WHERE id2 = master.id;
DELETE FROM mb_release_group_link WHERE id IS NULL;
ALTER TABLE mb_release_group_link DROP COLUMN id2;

SELECT t1.gid, t1.name, t1.url, lower(decodeURL(t1.url, 'http://www.discogs.com/artist/')) AS id2 
INTO mb_artist_link 
FROM dblink(:dblink, 'SELECT gid, name, url FROM do_artist_link') AS t1(gid uuid, name text, url text);
ALTER TABLE mb_artist_link ADD COLUMN id integer;
UPDATE mb_artist_link SET id = artist.id FROM artist WHERE id2 = lower(artist.name);
DELETE FROM mb_artist_link WHERE id IS NULL;
ALTER TABLE mb_artist_link DROP COLUMN id2;

SELECT t1.gid, t1.name, t1.url, lower(decodeURL(t1.url, 'http://www.discogs.com/label/')) AS id2 
INTO mb_label_link 
FROM dblink(:dblink, 'SELECT gid, name, url FROM do_label_link') AS t1(gid uuid, name text, url text);
ALTER TABLE mb_label_link ADD COLUMN id integer;
UPDATE mb_label_link SET id = label.id FROM label WHERE id2 = lower(label.name);
DELETE FROM mb_label_link WHERE id IS NULL;
ALTER TABLE mb_label_link DROP COLUMN id2;
"""
        self.dodb.execute(text(doquery), dblink=cfg.MB_DB_LINK)
        self.close()

    def create_track_count(self):
        self.open(do=True)
        doquery = """
SELECT release_id, COUNT(track_id)
INTO release_track_count FROM track
GROUP BY release_id;
"""
        self.dodb.execute(doquery)
        self.close()

    def createfunctions(self):
        self.open(do=True)
        doquery = """
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

CREATE OR REPLACE FUNCTION artist_name(name text)
  RETURNS text
AS $$
  import re
  global name
  result = re.match("(.+?)( \(\d+\))?$",name)
  return result.group(1)
$$ LANGUAGE plpython3u;

UPDATE release SET country = 'United States' WHERE country = 'US';
UPDATE release SET country = 'United Kingdom' WHERE country = 'UK';
"""
        self.dodb.execute(doquery)
        self.close()

    def create_link_views(self):
        self.open(mb=True)
        mbquery = """
DROP INDEX IF EXISTS l_release_url_idx_discogs, l_release_group_url_idx_discogs, l_label_url_idx_discogs, l_artist_url_idx_discogs;

CREATE INDEX l_release_url_idx_discogs ON l_release_url(link) WHERE link = 6301;
CREATE INDEX l_release_group_url_idx_discogs ON l_release_group_url(link) WHERE link = 6309;
CREATE INDEX l_label_url_idx_discogs ON l_label_url(link) WHERE link = 27037;
CREATE INDEX l_artist_url_idx_discogs ON l_artist_url(link) WHERE link = 26038;

CREATE OR REPLACE VIEW do_release_link AS
SELECT release.id, release_name.name, release.gid, url.url
FROM l_release_url
JOIN url ON l_release_url.entity1 = url.id
JOIN release ON l_release_url.entity0 = release.id
JOIN release_name ON release.name = release_name.id
WHERE l_release_url.link = 6301;

CREATE OR REPLACE VIEW do_release_group_link AS
SELECT release_group.id, release_name.name, release_group.gid, url.url
FROM l_release_group_url
JOIN url ON l_release_group_url.entity1 = url.id
JOIN release_group ON l_release_group_url.entity0 = release_group.id
JOIN release_name ON release_group.name = release_name.id
WHERE l_release_group_url.link = 6309;

CREATE OR REPLACE VIEW do_label_link AS
SELECT label.id, label_name.name, label.gid, url.url
FROM l_label_url
JOIN url ON l_label_url.entity1 = url.id
JOIN label ON l_label_url.entity0 = label.id
JOIN label_name ON label.name = label_name.id
WHERE l_label_url.link = 27037;

CREATE OR REPLACE VIEW do_artist_link AS
SELECT artist.id, artist_name.name, artist.gid, url.url
FROM l_artist_url
JOIN url ON l_artist_url.entity1 = url.id
JOIN artist ON l_artist_url.entity0 = artist.id
JOIN artist_name ON artist.name = artist_name.id
WHERE l_artist_url.link = 26038;
"""
        self.mbdb.execute(mbquery)
        self.close()

    def release_link_table(self):
        self.open(do=True, mb=True)
        mbquery = """
DROP TABLE IF EXISTS do_release_link_catno;
SELECT do_label_link.name AS label_name, do_label_link.gid AS label_gid, release_label.catalog_number, release.gid, 
release_name.name, tracklist.track_count, medium_format.name AS format, country.name AS country
INTO do_release_link_catno
FROM do_label_link
JOIN release_label ON do_label_link.id = release_label.label
JOIN release ON release_label.release = release.id
JOIN country ON release.country = country.id
JOIN release_name ON release.name = release_name.id
JOIN medium ON medium.release = release.id
JOIN tracklist ON medium.tracklist = tracklist.id
LEFT JOIN medium_format ON medium.format = medium_format.id
WHERE release_label.catalog_number NOTNULL;

DELETE FROM do_release_link_catno USING do_release_link WHERE do_release_link_catno.gid = do_release_link.gid;

DELETE FROM do_release_link_catno WHERE catalog_number IN (
  SELECT catalog_number FROM do_release_link_catno
  GROUP BY catalog_number
  HAVING (COUNT(catalog_number) > 1)
);
"""
        mbquery_cleanup = "DROP TABLE IF EXISTS do_release_link_catno"
        doquery = """
DROP TABLE IF EXISTS rel_catno, discogs_db_release_link;

WITH mb_release_link_catno AS (
    SELECT DISTINCT t1.* FROM dblink(:dblink, 'SELECT label_gid, label_name, catalog_number, gid, name, track_count, format, country FROM do_release_link_catno')
    AS t1(label_gid uuid, label_name text, catalog_number text, gid uuid, name text, track_count integer, format text, country text)
)
SELECT DISTINCT mb_label_link.id AS label_id, mb_release_link_catno.*, releases_labels.release_id
INTO rel_catno 
FROM mb_release_link_catno
JOIN mb_label_link ON mb_release_link_catno.label_gid = mb_label_link.gid
JOIN label ON mb_label_link.id = label.id
JOIN releases_labels ON label.name = releases_labels.label
WHERE releases_labels.catno = mb_release_link_catno.catalog_number;

UPDATE rel_catno SET format = 'File' WHERE format = 'Digital Media';
UPDATE rel_catno SET format = 'Minidisc' WHERE format = 'MiniDisc';
UPDATE rel_catno SET format = 'CDr' WHERE format = 'CR-R';

DELETE FROM rel_catno
WHERE catalog_number IN (
  SELECT catalog_number FROM rel_catno
  GROUP BY catalog_number
  HAVING (COUNT(catalog_number) > 1)
);

DELETE FROM rel_catno
WHERE gid IN (
  SELECT gid FROM rel_catno
  GROUP BY gid
  HAVING (COUNT(gid) > 1)
);

WITH rel_catno2 AS (
    SELECT DISTINCT rel_catno.* FROM rel_catno
    JOIN release ON rel_catno.release_id = release.id
    JOIN release_track_count ON release_track_count.release_id = release.id
    JOIN releases_formats ON releases_formats.release_id = release.id
    WHERE upper(release.title) = upper(rel_catno.name)
    AND release_track_count.count = track_count
    AND format = releases_formats.format_name
    AND rel_catno.country = release.country
)
SELECT gid, 'http://www.discogs.com/release/' || release_id AS url, 'Label "' || label_name || '": http://musicbrainz.org/label/' 
|| label_gid || ' have Discogs link.
It collection contains release "' || name || '": http://musicbrainz.org/release/' 
|| gid || ' with unique catalog number "' || catalog_number || '".
Collection of linked Discogs label contains release http://www.discogs.com/release/'
|| release_id  || ' with same unique catalog number, case insensitive match on release name, same number of tracks, same format and same release country' AS note
INTO discogs_db_release_link
FROM rel_catno2
ORDER BY label_name;

DROP TABLE IF EXISTS rel_catno;
"""
        self.mbdb.execute(mbquery)
        print 'MB side done!'
        self.dodb.execute(text(doquery), dblink=cfg.MB_DB_LINK)
        self.mbdb.execute(mbquery_cleanup)
        self.close()

    def artist_link_table(self):
        self.open(do=True, mb=True)
        mbquery = """
DROP TABLE IF EXISTS do_artist_releases;
SELECT do_release_link.id AS release_id, do_release_link.gid AS release_gid, do_release_link.name AS release_name, 
do_release_link.url AS release_url, artist.id AS artist_id, artist.gid AS artist_gid, artist_name.name AS artist_name
INTO do_artist_releases
FROM do_release_link
JOIN release ON do_release_link.id = release.id
JOIN artist_credit ON release.artist_credit = artist_credit.id
JOIN artist_name ON artist_credit.name = artist_name.id
JOIN artist_credit_name ON artist_credit_name.artist_credit = artist_credit.id
JOIN artist ON artist_credit_name.artist = artist.id
WHERE artist_credit.artist_count = 1
AND artist.id > 1
AND artist.id NOT IN (SELECT id FROM do_artist_link);
"""
        mbquery_cleanup = "DROP TABLE IF EXISTS do_artist_releases"
        doquery = """
DROP TABLE IF EXISTS mb_artist_releases;

SELECT DISTINCT t1.* 
INTO mb_artist_releases 
FROM dblink(:dblink, 'SELECT release_id, release_gid, release_name, release_url, artist_id, artist_gid, artist_name FROM do_artist_releases')
AS t1(release_id integer, release_gid uuid, release_name text, release_url text, artist_id integer, artist_gid uuid, artist_name text);

DELETE FROM mb_artist_releases
WHERE release_gid IN (
  SELECT release_gid FROM mb_artist_releases
  GROUP BY release_gid
  HAVING (COUNT(release_gid) > 1)
);

DROP TABLE IF EXISTS possible_artist_mblink2;

SELECT DISTINCT mb_artist_releases.artist_name, artist.name as discogs_artist_name, mb_artist_releases.artist_gid
INTO possible_artist_mblink2 
FROM mb_artist_releases
JOIN mb_release_link ON mb_artist_releases.release_gid = mb_release_link.gid
JOIN releases_artists ON mb_release_link.id = releases_artists.release_id
JOIN artist ON releases_artists.artist_id = artist.id
WHERE artist.name!='Various';

DROP TABLE IF EXISTS possible_artist_mblink3;

SELECT * INTO possible_artist_mblink3 FROM possible_artist_mblink2
WHERE artist_gid IN (
  SELECT artist_gid FROM possible_artist_mblink2
  GROUP BY artist_gid
  HAVING (COUNT(artist_gid) = 1)
) AND artist_name = artist_name(discogs_artist_name);

DROP TABLE IF EXISTS mb_artist_releases2;
SELECT *, release_name || ': http://musicbrainz.org/release/' || release_gid AS rel INTO mb_artist_releases2 FROM mb_artist_releases;

DROP TABLE IF EXISTS discogs_db_artist_link;

SELECT possible_artist_mblink3.artist_gid, url(possible_artist_mblink3.discogs_artist_name), 
(SELECT string_agg(mb_artist_releases2.rel,' , ') 
FROM mb_artist_releases2 
WHERE possible_artist_mblink3.artist_gid = mb_artist_releases2.artist_gid) AS releases
INTO discogs_db_artist_link
FROM possible_artist_mblink3;

DROP TABLE IF EXISTS possible_artist_mblink2, possible_artist_mblink3, mb_artist_releases, mb_artist_releases2;
"""
        self.mbdb.execute(mbquery)
        print 'MB side done!'
        self.dodb.execute(text(doquery), dblink=cfg.MB_DB_LINK)
        #self.mbdb.execute(mbquery_cleanup)
        self.close()

    def release_artist_link_table(self):
        self.open(do=True, mb=True)
        doquery = """
DROP TABLE IF EXISTS mb_release_artist_credits;

SELECT mb_release_link.gid AS release_gid, mb_release_link.url AS release_url, mb_artist_link.gid AS artist_gid, mb_artist_link.url AS artist_url, releases_extraartists.roles
INTO mb_release_artist_credits
FROM mb_release_link
JOIN releases_extraartists ON mb_release_link.id = releases_extraartists.release_id
JOIN artist ON releases_extraartists.artist_id = artist.id
JOIN mb_artist_link ON mb_artist_link.id = artist.id;

DELETE FROM mb_release_artist_credits WHERE release_gid IN
(SELECT gid FROM mb_release_link GROUP BY gid HAVING count(gid) > 1);

DELETE FROM mb_release_artist_credits WHERE artist_gid IN
(SELECT gid FROM mb_artist_link GROUP BY gid HAVING count(gid) > 1);
"""
        mbquery = """
DROP TABLE IF EXISTS do_release_artist_credits;

WITH do_rac AS (
    SELECT *
    FROM dblink(:dblink, 'SELECT release_gid, release_url, artist_gid, artist_url, roles FROM mb_release_artist_credits')
    AS t1(release_gid uuid, release_url text, artist_gid uuid, artist_url text, roles text[])
    WHERE 'Producer' = ANY(roles)
),
included AS (
	SELECT do_rac.artist_gid, do_rac.release_gid FROM do_rac
	EXCEPT
	SELECT artist.gid AS artist_gid, release.gid AS release_gid
	FROM link_type 
	JOIN link ON link.link_type = link_type.id
	JOIN l_artist_release ON l_artist_release.link = link.id
	JOIN artist ON l_artist_release.entity0 = artist.id
	JOIN release ON l_artist_release.entity1 = release.id
	WHERE link_type.name = 'producer'
)
SELECT do_rac.*
INTO do_release_artist_credits
FROM do_rac
JOIN included ON included.artist_gid = do_rac.artist_gid AND included.release_gid = do_rac.release_gid;
"""
        self.dodb.execute(doquery)
        print 'Discogs side done!'
        self.mbdb.execute(text(mbquery), dblink=cfg.DO_DB_LINK)
        self.close()

    def open(self, mb=False, do=False, client=False):
        if mb: self.mbdb = self.mbengine.connect()
        if do: self.dodb = self.doengine.connect()
        if client: return MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE)
        return None

    def close(self):
        if hasattr(self, 'mbdb'): self.mbdb.close()
        if hasattr(self, 'dodb'): self.dodb.close()
