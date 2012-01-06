import config as cfg
import sqlalchemy
from sqlalchemy.sql import text
from editing import MusicBrainzClient

class DiscogsDbClient(object):
    
    def __init__(self):
        self.mbengine = sqlalchemy.create_engine(cfg.MB_DB)
        self.mbdb = self.mbengine.connect()
        self.doengine = sqlalchemy.create_engine(cfg.DO_DB)
        self.dodb = self.doengine.connect()

    def commit_artist_links(self, limit):
        mbClient = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE)
        queryLinks = "SELECT artist_gid, url, releases FROM discogs_db_artist_link LIMIT %s"
        queryDelete = "DELETE FROM discogs_db_artist_link WHERE artist_gid = %s"
        for gid, url, releases in self.dodb.execute(queryLinks, limit):
            note = "Exact match on name.\nArtist have at least one release (" + releases + ") that have Discogs url.\n"\
                + "All releases found that way point on same artist at Discogs."
            mbClient.add_url("artist", gid, 180, "http://www.discogs.com/artist/" + url, note)
            self.dodb.execute(queryDelete, gid)
            print url + " Done!"

    def commit_release_links(self, limit):
        mbClient = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE)
        queryLinks = "SELECT gid, url, note FROM discogs_db_release_link LIMIT %s"
        queryDelete = "DELETE FROM discogs_db_release_link WHERE gid = %s"
        for gid, url, note in self.dodb.execute(queryLinks, limit):
            mbClient.add_url("release", gid, 76, url, note)
            self.dodb.execute(queryDelete, gid)
            print url + " Done!"

    def createlinks(self):
        mbquery = """
DROP TABLE IF EXISTS do_release_link, do_release_group_link, do_artist_link, do_label_link;
-- release: links
SELECT release.id, release_name.name, release.gid, url.url
INTO do_release_link FROM link
JOIN l_release_url ON  l_release_url.link = link.id
JOIN url ON l_release_url.entity1 = url.id
JOIN release ON l_release_url.entity0 = release.id
JOIN release_name ON release.name = release_name.id
WHERE link.link_type = 76
AND url.url LIKE '%%discogs.com/release/%%';

-- release group: links
SELECT release_group.id, release_name.name, release_group.gid, url.url
INTO do_release_group_link FROM link
JOIN l_release_group_url ON l_release_group_url.link = link.id
JOIN url ON l_release_group_url.entity1 = url.id
JOIN release_group ON l_release_group_url.entity0 = release_group.id
JOIN release_name ON release_group.name = release_name.id
WHERE link.link_type = 90
AND url.url LIKE '%%discogs.com/master/%%';

-- artist: links
SELECT artist.id, artist_name.name, artist.gid, url.url
INTO do_artist_link FROM link
JOIN l_artist_url ON l_artist_url.link = link.id
JOIN url ON l_artist_url.entity1 = url.id
JOIN artist ON l_artist_url.entity0 = artist.id
JOIN artist_name ON artist.name = artist_name.id
WHERE link.link_type = 180
AND url.url LIKE '%%discogs.com/artist/%%';

-- label: links
SELECT label.id, label_name.name, label.gid, url.url
INTO do_label_link FROM link
JOIN l_label_url ON l_label_url.link = link.id
JOIN url ON l_label_url.entity1 = url.id
JOIN label ON l_label_url.entity0 = label.id
JOIN label_name ON label.name = label_name.id
WHERE link.link_type = 217
AND url.url LIKE '%%discogs.com/label/%%';
"""
        doquery = """
DROP TABLE IF EXISTS mb_release_link, mb_release_group_link, mb_artist_link, mb_label_link;

SELECT t1.gid, t1.name, t1.url, decodeURL(t1.url, 'http://www.discogs.com/release/') AS id2 
INTO mb_release_link 
FROM dblink(:dblink, 'SELECT gid, name, url FROM do_release_link') AS t1(gid uuid, name text, url text);
ALTER TABLE mb_release_link ADD COLUMN id integer;
UPDATE mb_release_link SET id = release.id FROM release WHERE id2 = release.id::text;
DELETE FROM mb_release_link WHERE id IS NULL;
ALTER TABLE mb_release_link DROP COLUMN id2;

SELECT t1.gid, t1.name, t1.url, decodeURL(t1.url, 'http://www.discogs.com/master/') AS id2 
INTO mb_release_group_link 
FROM dblink(:dblink, 'SELECT gid, name, url FROM do_release_group_link') AS t1(gid uuid, name text, url text);
ALTER TABLE mb_release_group_link ADD COLUMN id text;
UPDATE mb_release_group_link SET id = master.id FROM master WHERE id2::integer = master.id;
DELETE FROM mb_release_group_link WHERE id IS NULL;
ALTER TABLE mb_release_group_link DROP COLUMN id2;

SELECT t1.gid, t1.name, t1.url, decodeURL(t1.url, 'http://www.discogs.com/artist/') AS id2 
INTO mb_artist_link 
FROM dblink(:dblink, 'SELECT gid, name, url FROM do_artist_link') AS t1(gid uuid, name text, url text);
ALTER TABLE mb_artist_link ADD COLUMN id integer;
UPDATE mb_artist_link SET id = artist.id FROM artist WHERE id2 = artist.name;
DELETE FROM mb_artist_link WHERE id IS NULL;
ALTER TABLE mb_artist_link DROP COLUMN id2;

SELECT t1.gid, t1.name, t1.url, decodeURL(t1.url, 'http://www.discogs.com/label/') AS id2 
INTO mb_label_link 
FROM dblink(:dblink, 'SELECT gid, name, url FROM do_label_link') AS t1(gid uuid, name text, url text);
ALTER TABLE mb_label_link ADD COLUMN id integer;
UPDATE mb_label_link SET id = label.id FROM label WHERE id2 = label.name;
DELETE FROM mb_label_link WHERE id IS NULL;
ALTER TABLE mb_label_link DROP COLUMN id2;
"""
        self.mbdb.execute(mbquery)
        self.dodb.execute(text(doquery), dblink=cfg.MB_DB_LINK)

    def create_track_count(self):
        doquery = """
SELECT release_id, COUNT(track_id)
INTO release_track_count FROM track
GROUP BY release_id;
"""
        self.dodb.execute(doquery)

    def createfunctions(self):
        doquery = """
-- function to decode url
CREATE OR REPLACE FUNCTION decodeUrl(url text, remove text)
  RETURNS text
AS $$
  import urllib.parse
  global url, remove
  result = urllib.parse.unquote_plus(url)
  return result[len(remove):]
$$ LANGUAGE plpython3u;

-- function to encode name to discogs url
CREATE OR REPLACE FUNCTION url(name text)
  RETURNS text
AS $$
  import urllib.parse
  global name
  encoded = urllib.parse.quote_plus(name,'()')
  return encoded
$$ LANGUAGE plpython3u;
"""
        self.dodb.execute(doquery)

    def release_link_table(self):
        mbquery = """
DROP TABLE IF EXISTS do_release_link_catno;
-- all releases linked to linked labels that have catalog_number
SELECT do_label_link.name AS label_name, do_label_link.gid AS label_gid, release_label.catalog_number, release.gid, 
release_name.name, tracklist.track_count, medium_format.name AS format
INTO do_release_link_catno
FROM do_label_link
JOIN release_label ON do_label_link.id = release_label.label
JOIN release ON release_label.release = release.id
JOIN release_name ON release.name = release_name.id
JOIN medium ON medium.release = release.id
JOIN tracklist ON medium.tracklist = tracklist.id
LEFT JOIN medium_format ON medium.format = medium_format.id
WHERE release_label.catalog_number NOTNULL;

-- delete releases that are already linked to discogs
DELETE FROM do_release_link_catno USING do_release_link WHERE do_release_link_catno.gid = do_release_link.gid;

-- delete releases with non unique catalog numbers
DELETE FROM do_release_link_catno WHERE catalog_number IN (
  SELECT catalog_number FROM do_release_link_catno
  GROUP BY catalog_number
  HAVING (COUNT(catalog_number) > 1)
);
"""
        mbquery_cleanup = "DROP TABLE IF EXISTS do_release_link_catno"
        doquery = """
DROP TABLE IF EXISTS rel_catno, discogs_db_release_link;

-- find releases that are releted to same label and have same catalog_number
WITH mb_release_link_catno AS (
    SELECT DISTINCT t1.* FROM dblink(:dblink, 'SELECT label_gid, label_name, catalog_number, gid, name, track_count, format FROM do_release_link_catno')
    AS t1(label_gid uuid, label_name text, catalog_number text, gid uuid, name text, track_count integer, format text)
)
SELECT DISTINCT mb_label_link.id AS label_id, mb_release_link_catno.*, releases_labels.release_id
INTO rel_catno 
FROM mb_release_link_catno
JOIN mb_label_link ON mb_release_link_catno.label_gid = mb_label_link.gid
JOIN label ON mb_label_link.id = label.id
JOIN releases_labels ON label.name = releases_labels.label
WHERE releases_labels.catno = mb_release_link_catno.catalog_number;

-- remove releases with non unique catalog_numbers
DELETE FROM rel_catno
WHERE catalog_number IN (
  SELECT catalog_number FROM rel_catno
  GROUP BY catalog_number
  HAVING (COUNT(catalog_number) > 1)
);

-- remove releases with non unique gid
DELETE FROM rel_catno
WHERE gid IN (
  SELECT gid FROM rel_catno
  GROUP BY gid
  HAVING (COUNT(gid) > 1)
);

-- change some formats so they can be matched
UPDATE rel_catno SET format = 'File' WHERE format = 'Digital Media';
UPDATE rel_catno SET format = 'Minidisc' WHERE format = 'MiniDisc';
UPDATE rel_catno SET format = 'CDr' WHERE format = 'CR-R';

WITH rel_catno2 AS (
    SELECT DISTINCT rel_catno.* FROM rel_catno
    JOIN release ON rel_catno.release_id = release.id
    JOIN release_track_count ON release_track_count.release_id = release.id
    JOIN releases_formats ON releases_formats.release_id = release.id
    WHERE upper(release.title) = upper(rel_catno.name)
    AND release_track_count.count = track_count
    AND format = releases_formats.format_name
)
SELECT gid, 'http://www.discogs.com/release/' || release_id AS url, 'Label "' || label_name || '": http://musicbrainz.org/label/' 
|| label_gid || ' have Discogs link.
It collection contains release "' || name || '": http://musicbrainz.org/release/' 
|| gid || ' with unique catalog number "' || catalog_number || '".
Collection of linked Discogs label contains release http://www.discogs.com/release/'
|| release_id  || ' with same unique catalog number, case insensitive match on release name, same number of tracks and same format' AS note
INTO discogs_db_release_link
FROM rel_catno2
ORDER BY label_name;

DROP TABLE IF EXISTS rel_catno;
"""
        self.mbdb.execute(mbquery)
        self.dodb.execute(text(doquery), dblink=cfg.MB_DB_LINK)
        self.mbdb.execute(mbquery_cleanup)

    def artist_link_table(self):
        mbquery = """
-- releases linked to Discogs with artist not linked to Discogs
DROP TABLE IF EXISTS do_artist_releases;
-- all releases with Discogs link expanded to artist_credits where only one artist is credited
SELECT do_release_link.id AS release_id, do_release_link.gid AS release_gid, do_release_link.name AS release_name, 
do_release_link.url AS release_url, artist.id AS artist_id, artist.gid AS artist_gid, artist_name.name AS artist_name
INTO do_artist_releases FROM do_release_link
JOIN release ON do_release_link.id = release.id
JOIN artist_credit ON release.artist_credit = artist_credit.id
JOIN artist_name ON artist_credit.name = artist_name.id
JOIN artist_credit_name ON artist_credit_name.artist_credit = artist_credit.id
JOIN artist ON artist_credit_name.artist = artist.id
WHERE artist_credit.artist_count = 1;

-- delete artists that are already linked to Discogs
DELETE FROM do_artist_releases
WHERE artist_id 
IN (SELECT artist.id FROM l_artist_url 
    JOIN artist ON l_artist_url.entity0 = artist.id
    JOIN link ON l_artist_url.link = link.id
    WHERE link.link_type = 180);
"""
        mbquery_cleanup = "DROP TABLE IF EXISTS do_artist_releases"
        doquery = """
-- artist links
DROP TABLE IF EXISTS mb_artist_releases;

SELECT DISTINCT t1.* 
INTO mb_artist_releases 
FROM dblink(:dblink, 'SELECT release_id, release_gid, release_name, release_url, artist_id, artist_gid, artist_name FROM do_artist_releases')
AS t1(release_id integer, release_gid uuid, release_name text, release_url text, artist_id integer, artist_gid uuid, artist_name text);

-- remove 'Various Artists' links
DELETE FROM mb_artist_releases WHERE artist_id = 1;

-- delete all that have multiple entries
DELETE FROM mb_artist_releases
WHERE release_gid IN (
  SELECT release_gid FROM mb_artist_releases
  GROUP BY release_gid
  HAVING (COUNT(release_gid) > 1)
);

DROP TABLE IF EXISTS possible_artist_mblink2;

SELECT DISTINCT mb_artist_releases.artist_name, releases_artists.artist_name as discogs_artist_name, mb_artist_releases.artist_gid
INTO possible_artist_mblink2 
FROM mb_artist_releases
JOIN mb_release_link ON mb_artist_releases.release_gid = mb_release_link.gid
JOIN releases_artists ON mb_release_link.id = releases_artists.release_id
WHERE releases_artists.artist_name!='Various';

DROP TABLE IF EXISTS possible_artist_mblink3;

-- one to one mapped with exact name match
SELECT * INTO possible_artist_mblink3 FROM possible_artist_mblink2
WHERE artist_gid IN (
  SELECT artist_gid FROM possible_artist_mblink2
  GROUP BY artist_gid
  HAVING (COUNT(artist_gid) = 1)
) AND artist_name = discogs_artist_name;

DROP TABLE IF EXISTS mb_artist_releases2;
SELECT *, release_name || ': http://musicbrainz.org/release/' || release_gid AS rel INTO mb_artist_releases2 FROM mb_artist_releases;

DROP TABLE IF EXISTS discogs_db_artist_link;
-- possible artis links
SELECT possible_artist_mblink3.artist_gid, url(possible_artist_mblink3.artist_name), 
(SELECT string_agg(mb_artist_releases2.rel,' , ') 
FROM mb_artist_releases2 
WHERE possible_artist_mblink3.artist_gid = mb_artist_releases2.artist_gid) AS releases
INTO discogs_db_artist_link
FROM possible_artist_mblink3;

DROP TABLE IF EXISTS possible_artist_mblink2, possible_artist_mblink3, mb_artist_releases, mb_artist_releases2;
"""
        self.mbdb.execute(mbquery)
        self.dodb.execute(text(doquery), dblink=cfg.MB_DB_LINK)
        self.mbdb.execute(mbquery_cleanup)