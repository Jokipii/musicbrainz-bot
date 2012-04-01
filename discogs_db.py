import config as cfg
import sqlalchemy
from sqlalchemy.sql import text
from editing import MusicBrainzClient
from datetime import date

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
        results = self.dodb.execute(queryLinks, limit).fetchall()
        for gid, url, note in results:
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

    def commit_artist_country(self, limit):
        mbClient = self.open(mb=True, client=True)
        query = """
WITH countries AS (
	SELECT id, array_cat(array[lower(name)||'%%'],country_search.search) AS search FROM country 
	LEFT JOIN country_search ON country.iso_code = country_search.iso_code
)
SELECT countries.id AS country_id, s_artist.gid, s_artist.name, comment FROM s_artist
JOIN countries ON lower(comment) LIKE ANY(search)
AND country ISNULL
AND comment NOT LIKE '%%?%%'
AND comment !~* '^[a-z]+[-/]'
LIMIT %s
"""
        results = self.mbdb.execute(query, limit).fetchall()
        self.close()
        for country_id, gid, name, comment in results:
            note = "Based on disambiguation comment"
            mbClient.set_artist_country_id(gid, country_id, note)
            print gid + " Done!"

    def commit_artist_country2(self, limit):
        mbClient = self.open(do=True, client=True)
        query = """
WITH countries AS (
	SELECT t1.* 
	FROM dblink(:dblink, '
		SELECT id, array_cat(array[lower(name)||''%%''],country_search.search) AS search FROM country 
		LEFT JOIN country_search ON country.iso_code = country_search.iso_code
	') AS t1(id integer, search text[])
), gids AS (
	SELECT t1.* 
	FROM dblink(:dblink, '
		SELECT do_artist_link.gid 
		FROM do_artist_link
		JOIN artist ON artist.id = do_artist_link.id
		WHERE artist.country ISNULL
	') AS t1(gid uuid)
)
SELECT countries.id AS country_id, mb_artist_link.gid, mb_artist_link.name, substring(artist.profile FROM '^[^.]+\.?') AS comment
FROM mb_artist_link
JOIN artist ON mb_artist_link.id = artist.id
JOIN gids ON gids.gid = mb_artist_link.gid
JOIN countries ON lower(artist.profile) LIKE ANY(search)
WHERE artist.profile NOT LIKE '%%?%%'
AND artist.profile !~* '^[a-z]+[-/]'
LIMIT :limit
"""
        results = self.dodb.execute(text(query), dblink=cfg.MB_DB_LINK, limit=limit).fetchall()
        self.close()
        for country_id, gid, name, comment in results:
            note = "Based on Discogs profile: " + comment
            mbClient.set_artist_country_id(gid, country_id, note)
            print gid + " Done!"

    def report(self):
        self.open(mb=True, do=True)
        print '{|\n! ' + date.today().isoformat()
        print """! MusicBrainz Total 
! Discogs Total 
! Links (all these are not unique) 
! Percent done (compared to smaller total) 
|-"""
        mb = self.mbdb.execute('SELECT COUNT(id) FROM release').fetchone()[0]
        do = self.dodb.execute('SELECT COUNT(id) FROM release').fetchone()[0]
        link = self.mbdb.execute('SELECT COUNT(id) FROM do_release_link').fetchone()[0]
        perc = float(link)/float(mb)*100.0
        print '! Releases:\n| '+ str(mb) +'\n| '+ str(do) +'\n| '+ str(link) +('\n| %0.f' % perc) +'%\n|-'
        mb = self.mbdb.execute('SELECT COUNT(id) FROM release_group').fetchone()[0]
        do = self.dodb.execute('SELECT COUNT(id) FROM master').fetchone()[0]
        link = self.mbdb.execute('SELECT COUNT(id) FROM do_release_group_link').fetchone()[0]
        perc = float(link)/float(do)*100.0
        print '! Release Groups:\n| '+ str(mb) +'\n| '+ str(do) +'\n| '+ str(link) +('\n| %0.f' % perc) +'%\n|-'
        mb = self.mbdb.execute('SELECT COUNT(id) FROM artist').fetchone()[0]
        do = self.dodb.execute('SELECT COUNT(id) FROM artist').fetchone()[0]
        link = self.mbdb.execute('SELECT COUNT(id) FROM do_artist_link').fetchone()[0]
        perc = float(link)/float(mb)*100.0
        print '! Artists:\n| '+ str(mb) +'\n| '+ str(do) +'\n| '+ str(link) +('\n| %0.f' % perc) +'%\n|-'
        mb = self.mbdb.execute('SELECT COUNT(id) FROM label').fetchone()[0]
        do = self.dodb.execute('SELECT COUNT(id) FROM label').fetchone()[0]
        link = self.mbdb.execute('SELECT COUNT(id) FROM do_label_link').fetchone()[0]
        perc = float(link)/float(mb)*100.0
        print '! Labels:\n| '+ str(mb) +'\n| '+ str(do) +'\n| '+ str(link) +('\n| %0.f' % perc) +'%\n|}'
        self.close()

    def createlinks(self):
        self.open(do=True)
        doquery = """
DROP TABLE IF EXISTS mb_release_link, mb_release_group_link, mb_artist_link, mb_label_link;

SELECT t1.gid, t1.name, t1.url, (regexp_matches(t1.url, '(?:^http://www.discogs.com/release/)([0-9]+)'))[1]::integer AS id2
INTO mb_release_link 
FROM dblink(:dblink, 'SELECT gid, name, url FROM do_release_link WHERE url ~ ''^http://www.discogs.com/release/''')
AS t1(gid uuid, name text, url text);
ALTER TABLE mb_release_link ADD COLUMN id integer;
UPDATE mb_release_link SET id = release.id FROM release WHERE id2 = release.id;
DELETE FROM mb_release_link WHERE id IS NULL;
ALTER TABLE mb_release_link DROP COLUMN id2;

SELECT t1.gid, t1.name, t1.url, (regexp_matches(t1.url, '(?:^http://www.discogs.com/master/)([0-9]+)'))[1]::integer AS id2
INTO mb_release_group_link 
FROM dblink(:dblink, 'SELECT gid, name, url FROM do_release_group_link WHERE url ~ ''^http://www.discogs.com/master/''')
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
 
    def create_media_type_mapping_table(self):
        """
        Method creates release_format table where format_id is same as MB
        medium_format.id and format_name is same as MB medium_format.name
        and release_id refers to Discogs release.id
        
        Method handles all Discogs formats except 'Book', which is intentionally
        excluded. Method uses format field and when necessary description field
        (http://www.discogs.com/help/formatslist) to find correct MB format.
        """
        self.open(do=True)
        query = """
DROP TABLE IF EXISTS release_format;

SELECT DISTINCT 34 as format_id, '8cm CD' as format_name, release_id
INTO release_format
FROM releases_formats 
WHERE format_name = ANY(array['CD','CDr']) AND 'Mini' = ANY(descriptions)
	UNION
SELECT DISTINCT 25 as format_id, 'HDCD' as format_name, release_id
FROM releases_formats 
WHERE format_name = ANY(array['CD','CDr']) AND 'HDCD' = ANY(descriptions)
	UNION
SELECT DISTINCT 23 as format_id, 'SVCD' as format_name, release_id
FROM releases_formats 
WHERE format_name = ANY(array['CD','CDr']) AND 'SVCD' = ANY(descriptions)
	UNION
SELECT DISTINCT 22 as format_id, 'VCD' as format_name, release_id
FROM releases_formats 
WHERE format_name = ANY(array['CD','CDr', 'CDV']) AND 'VCD' = ANY(descriptions)
	UNION
SELECT DISTINCT 3 as format_id, 'SACD' as format_name, release_id
FROM releases_formats 
WHERE format_name = 'CD' AND 'SACD' = ANY(descriptions)
	UNION
SELECT DISTINCT 1 as format_id, 'CD' as format_name, release_id
FROM releases_formats
WHERE format_name = 'CD'
AND 'SACD' <> ALL(descriptions)
AND 'VCD' <> ALL(descriptions)
AND 'SVCD' <> ALL(descriptions)
AND 'HDCD' <> ALL(descriptions)
AND 'Mini' <> ALL(descriptions)
	UNION
SELECT DISTINCT 33 as format_id, 'CD-R' as format_name, release_id
FROM releases_formats 
WHERE format_name = 'CDr'
AND 'VCD' <> ALL(descriptions)
AND 'SVCD' <> ALL(descriptions)
AND 'HDCD' <> ALL(descriptions)
AND 'Mini' <> ALL(descriptions)
	UNION
SELECT DISTINCT 31 as format_id, '12" Vinyl' as format_name, release_id
FROM releases_formats 
WHERE format_name = ANY(array['Vinyl', 'Flexi-disc', 'Acetate', 'Shellac', 'Lathe Cut', 'Edison Disc'])
AND '12"' = ANY(descriptions)
	UNION
SELECT DISTINCT 30 as format_id, '10" Vinyl' as format_name, release_id
FROM releases_formats 
WHERE format_name = ANY(array['Vinyl', 'Flexi-disc', 'Acetate', 'Shellac', 'Lathe Cut', 'Edison Disc'])
AND '10"' = ANY(descriptions)
	UNION
SELECT DISTINCT 29 as format_id, '7" Vinyl' as format_name, release_id
FROM releases_formats 
WHERE format_name = ANY(array['Vinyl', 'Flexi-disc', 'Acetate', 'Shellac', 'Lathe Cut', 'Edison Disc'])
AND '7"' = ANY(descriptions)
	UNION
SELECT DISTINCT 7 as format_id, 'Vinyl' as format_name, release_id
FROM releases_formats 
WHERE format_name = ANY(array['Vinyl', 'Flexi-disc', 'Acetate', 'Shellac', 'Lathe Cut', 'Edison Disc'])
AND '12"' <> ALL(descriptions)
AND '10"' <> ALL(descriptions)
AND '7"' <> ALL(descriptions)
	UNION
SELECT DISTINCT 18 as format_id, 'DVD-Audio' as format_name, release_id
FROM releases_formats 
WHERE format_name = ANY(array['DVD','DVDr'])
AND 'DVD-Audio' = ANY(descriptions)
	UNION
SELECT DISTINCT 19 as format_id, 'DVD-Video' as format_name, release_id
FROM releases_formats 
WHERE format_name = ANY(array['DVD','DVDr'])
AND 'DVD-Video' = ANY(descriptions)
AND 'DVD-Audio' <> ALL(descriptions)
	UNION
SELECT DISTINCT 2 as format_id, 'DVD' as format_name, release_id
FROM releases_formats 
WHERE format_name = ANY(array['DVD','DVDr'])
AND 'DVD-Video' <> ALL(descriptions)
AND 'DVD-Audio' <> ALL(descriptions)
	UNION
SELECT DISTINCT 4 as format_id, 'DualDisc' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['DualDisc'])
	UNION
SELECT DISTINCT 5 as format_id, 'LaserDisc' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['Laserdisc'])
	UNION
SELECT DISTINCT 6 as format_id, 'MiniDisc' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['Minidisc'])
	UNION
SELECT DISTINCT 8 as format_id, 'Casette' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['Cassette','Microcassette'])
	UNION
SELECT DISTINCT 9 as format_id, 'Cartridge' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['4-Track Cartridge','8-Track Cartridge'])
	UNION
SELECT DISTINCT 10 as format_id, 'Reel-to-reel' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['Reel-To-Reel'])
	UNION
SELECT DISTINCT 11 as format_id, 'DAT' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['DAT'])
	UNION
SELECT DISTINCT 12 as format_id, 'Digital Media' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['File'])
	UNION
SELECT DISTINCT 13 as format_id, 'Other' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['Datassette','MVD','Hybrid','SelectaVision','Floppy Disk','Box Set','All Media'])
	UNION
SELECT DISTINCT 14 as format_id, 'Wax Cylinder' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['Cylinder','Pathé Disc'])
	UNION
SELECT DISTINCT 16 as format_id, 'DCC' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['DCC'])
	UNION
SELECT DISTINCT 17 as format_id, 'HD-DVD' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['HD DVD','HD DVD-R'])
	UNION
SELECT DISTINCT 20 as format_id, 'Blu-ray' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['Blu-ray','Blu-ray-R'])
	UNION
SELECT DISTINCT 21 as format_id, 'VHS' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['VHS'])
	UNION
SELECT DISTINCT 24 as format_id, 'Betamax' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['Betamax'])
	UNION
SELECT DISTINCT 26 as format_id, 'USB Flash Drive' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['Memory Stick'])
	UNION
SELECT DISTINCT 28 as format_id, 'UMD' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['UMD'])
	UNION
SELECT DISTINCT 32 as format_id, 'Videotape' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['Video 2000'])
"""
        self.mbdb.execute(query)
        self.close()

    def create_release_status_table(self):
        """
        Method creates release_format table where format_id is same as MB
        medium_format.id and format_name is same as MB medium_format.name
        and release_id refers to Discogs release.id
        
        Method handles all Discogs formats except 'Book', which is intentionally
        excluded. Method uses format field and when necessary description field
        (http://www.discogs.com/help/formatslist) to find correct MB format.
        """
        self.open(do=True)
        query = """
DROP TABLE IF EXISTS release_status;

SELECT release_id, 1 AS status
INTO release_status
FROM releases_formats
WHERE ('Unofficial Release' <> ALL(descriptions) AND 'Partially Unofficial' <> ALL(descriptions))
AND ('Promo' <> ALL(descriptions) AND 'White Label' <> ALL(descriptions) AND 'Test Pressing' <> ALL(descriptions))
UNION
SELECT release_id, 2 AS status
FROM releases_formats
WHERE ('Promo' = ANY(descriptions) OR 'White Label' = ANY(descriptions) OR 'Test Pressing' = ANY(descriptions))
UNION
SELECT release_id, 3 AS status
FROM releases_formats
WHERE ('Unofficial Release' = ANY(descriptions) OR 'Partially Unofficial' = ANY(descriptions))
AND ('Promo' <> ALL(descriptions) AND 'White Label' <> ALL(descriptions) AND 'Test Pressing' <> ALL(descriptions))
"""
        self.mbdb.execute(query)
        self.close()

    def create_release_mb_mapping_table(self):
        self.open(do=True)
        query = """
UPDATE release SET released = NULL WHERE released !~ '^[0-9]{4}';

DROP TABLE IF EXISTS release_mb_mapping;

SELECT DISTINCT release.id AS release_id, release.title, left(release.released, 4)::smallint AS year, release_format.format_name, release_format.format_id,
	release_track_count.count AS track_count, release_status.status, country_mapping.id AS country_id, release.country
INTO release_mb_mapping
FROM release
LEFT JOIN release_format ON release_format.release_id = release.id
LEFT JOIN release_track_count ON release_track_count.release_id = release.id
LEFT JOIN release_status ON release_status.release_id = release.id
LEFT JOIN country_mapping ON country_mapping.name = release.country;
"""
        self.mbdb.execute(query)
        self.close()

    def create_country_mapping_table(self):
        """
        Created country mapping works on all (exception list below)
        countries that Discogs currently have in they database.
        
        Exceptions cannot be directly mapped on MB country codes.
        Discogs have 35.5k releases that use listed countries.
        2/3 of these releases have "UK & Europe", "Scandinavia", 
        or "USA & Canada" as they country.
        
        TODO: There must be some way to handle areas?
        
        Benelux	                            area
        Africa	                            area
        Asia	                            area
        Australasia	                        area
        Australia & New Zealand	            area
        Central America	                    area
        France & Benelux	                area
        Germany & Switzerland	            area
        Germany, Austria, & Switzerland	    area
        North America (inc Mexico)	        area
        Scandinavia	                        area
        South America	                    area
        UK & Europe	                        area
        UK & Ireland	                    area
        UK & US	                            area
        UK, Europe & US	                    area
        USA & Canada	                    area
        USA, Canada & UK	                area
        Gulf Cooperation Council	        area
        Ivory Coast	                        area
        Protectorate of Bohemia and Moravia historic
        Austria-Hungary	                    historic
        Virgin Islands	                    us or uk
        Korea	                            north or south
        """
        self.open(do=True)
        query = """
DROP TABLE IF EXISTS country_mapping;
CREATE TABLE country_mapping
(
  id integer,
  iso_code character varying(2) NOT NULL,
  name text NOT NULL
);
INSERT INTO country_mapping VALUES (NULL,'AG','Antigua & Barbuda');
INSERT INTO country_mapping VALUES (NULL,'AN','Netherlands Antilles');
INSERT INTO country_mapping VALUES (NULL,'BA','Bosnia & Herzegovina');
INSERT INTO country_mapping VALUES (NULL,'BN','Brunei');
INSERT INTO country_mapping VALUES (NULL,'BO','Bolivia');
INSERT INTO country_mapping VALUES (NULL,'BS','Bahamas, The');
INSERT INTO country_mapping VALUES (NULL,'CD','Congo, Democratic Republic of the');
INSERT INTO country_mapping VALUES (NULL,'CG','Congo, Republic of the');
INSERT INTO country_mapping VALUES (NULL,'CS','Serbia and Montenegro');
INSERT INTO country_mapping VALUES (NULL,'GB','UK');
INSERT INTO country_mapping VALUES (NULL,'IR','Iran');
INSERT INTO country_mapping VALUES (NULL,'KP','North Korea');
INSERT INTO country_mapping VALUES (NULL,'KR','South Korea');
INSERT INTO country_mapping VALUES (NULL,'MD','Moldova');
INSERT INTO country_mapping VALUES (NULL,'MK','Macedonia');
INSERT INTO country_mapping VALUES (NULL,'MM','Burma');
INSERT INTO country_mapping VALUES (NULL,'MO','Macau');
INSERT INTO country_mapping VALUES (NULL,'PF','Clipperton Island');
INSERT INTO country_mapping VALUES (NULL,'PN','Pitcairn Islands');
INSERT INTO country_mapping VALUES (NULL,'RE','Reunion');
INSERT INTO country_mapping VALUES (NULL,'RU','Russia');
INSERT INTO country_mapping VALUES (NULL,'SU','USSR');
INSERT INTO country_mapping VALUES (NULL,'SY','Syria');
INSERT INTO country_mapping VALUES (NULL,'TF','French Southern & Antarctic Lands');
INSERT INTO country_mapping VALUES (NULL,'TF','Europa Island');
INSERT INTO country_mapping VALUES (NULL,'TL','East Timor');
INSERT INTO country_mapping VALUES (NULL,'TT','Trinidad & Tobago');
INSERT INTO country_mapping VALUES (NULL,'TZ','Tanzania');
INSERT INTO country_mapping VALUES (NULL,'UM','Wake Island');
INSERT INTO country_mapping VALUES (NULL,'UM','Kingman Reef');
INSERT INTO country_mapping VALUES (NULL,'US','US');
INSERT INTO country_mapping VALUES (NULL,'VA','Vatican City');
INSERT INTO country_mapping VALUES (NULL,'VA','Holy See (Vatican City)');
INSERT INTO country_mapping VALUES (NULL,'VC','Saint Vincent and the Grenadines');
INSERT INTO country_mapping VALUES (NULL,'VE','Venezuela');
INSERT INTO country_mapping VALUES (NULL,'VN','Vietnam');
INSERT INTO country_mapping VALUES (NULL,'XC','Czechoslovakia');
INSERT INTO country_mapping VALUES (NULL,'XG','German Democratic Republic (GDR)');
INSERT INTO country_mapping VALUES (NULL,'YU','Yugoslavia');
WITH country AS (
    SELECT id, iso_code, name 
    FROM dblink(:dblink, 'SELECT id, iso_code, name FROM country') 
    AS t1(id integer, iso_code character varying(2), name text)
)
UPDATE country_mapping SET id = (SELECT country.id FROM country WHERE country_mapping.iso_code = country.iso_code);
WITH country AS (
    SELECT id, iso_code, name 
    FROM dblink(:dblink, 'SELECT id, iso_code, name FROM country') 
    AS t1(id integer, iso_code character varying(2), name text)
)
INSERT INTO country_mapping SELECT * FROM country;
"""
        self.dodb.execute(text(query), dblink=cfg.MB_DB_LINK)
        self.close()

    def create_country_search_table(self):
        self.open(mb=True)
        mbquery = """
DROP TABLE IF EXISTS country_search;
CREATE TABLE country_search
(
  iso_code character varying(2) NOT NULL,
  search text[] NOT NULL,
  CONSTRAINT country_search_pkey PRIMARY KEY (iso_code)
);
INSERT INTO country_search VALUES ('AG', array['antigua & barbuda%']);
INSERT INTO country_search VALUES ('AN', array['netherlands antilles%']);
INSERT INTO country_search VALUES ('AR', array['argentinian%']);
INSERT INTO country_search VALUES ('AT', array['austrian%']);
INSERT INTO country_search VALUES ('AU', array['australian%']);
INSERT INTO country_search VALUES ('BA', array['bosnia & herzegovina%']);
INSERT INTO country_search VALUES ('BE', array['belgian%']);
INSERT INTO country_search VALUES ('BN', array['brunei%']);
INSERT INTO country_search VALUES ('BO', array['bolivia%']);
INSERT INTO country_search VALUES ('BR', array['brazilian%']);
INSERT INTO country_search VALUES ('BS', array['bahamas, the%']);
INSERT INTO country_search VALUES ('CA', array['canadian%']);
INSERT INTO country_search VALUES ('CD', array['congo, democratic republic of the%']);
INSERT INTO country_search VALUES ('CG', array['congo, republic of the%','republic of the congo%']);
INSERT INTO country_search VALUES ('CH', array['swiss%']);
INSERT INTO country_search VALUES ('CN', array['chinese%']);
INSERT INTO country_search VALUES ('CS', array['serbia and montenegro%']);
INSERT INTO country_search VALUES ('CU', array['cuban%']);
INSERT INTO country_search VALUES ('CZ', array['czech%']);
INSERT INTO country_search VALUES ('DE', array['german%']);
INSERT INTO country_search VALUES ('DK', array['danish%']);
INSERT INTO country_search VALUES ('DZ', array['algerian%']);
INSERT INTO country_search VALUES ('EE', array['estonian%']);
INSERT INTO country_search VALUES ('ES', array['spanish%']);
INSERT INTO country_search VALUES ('FI', array['finnish%']);
INSERT INTO country_search VALUES ('FR', array['french%']);
INSERT INTO country_search VALUES ('GB', array['uk %', 'english%', 'british%','scottish%','welsh%']);
INSERT INTO country_search VALUES ('GR', array['greek%']);
INSERT INTO country_search VALUES ('HK', array['hong kong%']);
INSERT INTO country_search VALUES ('HR', array['croatian%']);
INSERT INTO country_search VALUES ('HU', array['hungarian%']);
INSERT INTO country_search VALUES ('ID', array['indonesian%']);
INSERT INTO country_search VALUES ('IE', array['irish%']);
INSERT INTO country_search VALUES ('IL', array['israeli%']);
INSERT INTO country_search VALUES ('IN', array['indian%']);
INSERT INTO country_search VALUES ('IR', array['iranian%','iran%']);
INSERT INTO country_search VALUES ('IS', array['icelandic%']);
INSERT INTO country_search VALUES ('IT', array['italian%']);
INSERT INTO country_search VALUES ('JM', array['jamaican%']);
INSERT INTO country_search VALUES ('JP', array['japanese%']);
INSERT INTO country_search VALUES ('KP', array['north korea%']);
INSERT INTO country_search VALUES ('KR', array['south korean%','south korea%']);
INSERT INTO country_search VALUES ('LV', array['latvian%']);
INSERT INTO country_search VALUES ('MD', array['moldova%']);
INSERT INTO country_search VALUES ('MK', array['macedonia%']);
INSERT INTO country_search VALUES ('MM', array['burma%']);
INSERT INTO country_search VALUES ('MO', array['macau%']);
INSERT INTO country_search VALUES ('MX', array['mexican%']);
INSERT INTO country_search VALUES ('MZ', array['mozambican%']);
INSERT INTO country_search VALUES ('NL', array['dutch%']);
INSERT INTO country_search VALUES ('NO', array['norwegian%']);
INSERT INTO country_search VALUES ('NZ', array['new zealand%']);
INSERT INTO country_search VALUES ('PF', array['clipperton island%']);
INSERT INTO country_search VALUES ('PH', array['filipino%']);
INSERT INTO country_search VALUES ('PK', array['pakistani%']);
INSERT INTO country_search VALUES ('PL', array['polish%']);
INSERT INTO country_search VALUES ('PN', array['pitcairn islands%']);
INSERT INTO country_search VALUES ('PR', array['puerto rican%']);
INSERT INTO country_search VALUES ('PT', array['portugal%']);
INSERT INTO country_search VALUES ('RE', array['reunion%']);
INSERT INTO country_search VALUES ('RO', array['romanian%']);
INSERT INTO country_search VALUES ('RU', array['russian%','russia%']);
INSERT INTO country_search VALUES ('SE', array['swedish%']);
INSERT INTO country_search VALUES ('SG', array['singaporean%']);
INSERT INTO country_search VALUES ('SI', array['slovenian%']);
INSERT INTO country_search VALUES ('SK', array['slovak%']);
INSERT INTO country_search VALUES ('SN', array['senegalese%']);
INSERT INTO country_search VALUES ('SU', array['ussr%']);
INSERT INTO country_search VALUES ('SY', array['syria%']);
INSERT INTO country_search VALUES ('TL', array['east timor%']);
INSERT INTO country_search VALUES ('TR', array['turkish%']);
INSERT INTO country_search VALUES ('TZ', array['tanzania%']);
INSERT INTO country_search VALUES ('UA', array['ukrainian%']);
INSERT INTO country_search VALUES ('UM', array['wake island%','kingman reef%']);
INSERT INTO country_search VALUES ('US', array['us %', 'american%', 'usa %', 'los angeles %', 'san francisco %']);
INSERT INTO country_search VALUES ('VA', array['vatican city%','holy see (vatican city)%']);
INSERT INTO country_search VALUES ('VC', array['saint vincent and the grenadines%']);
INSERT INTO country_search VALUES ('VE', array['venezuela%']);
INSERT INTO country_search VALUES ('VN', array['vietnam%']);
INSERT INTO country_search VALUES ('XC', array['czechoslovakia%']);
INSERT INTO country_search VALUES ('XG', array['german democratic republic%']);
INSERT INTO country_search VALUES ('YU', array['yugoslavia%']);
INSERT INTO country_search VALUES ('ZA', array['south african%']);
"""
        self.mbdb.execute(text(mbquery))
        self.close()

    def release_link_table(self):
        self.open(do=True, mb=True)
        mbquery = """
DROP TABLE IF EXISTS do_release_link_catno;

WITH r AS (
    SELECT DISTINCT do_label_link.name AS label_name, do_label_link.gid AS label_gid, release_label.catalog_number, release.gid, 
        release_name.name, tracklist.track_count, medium.format, release.country, release.date_year AS year, 
        release.status, tracklist.id
    FROM do_label_link
    JOIN release_label ON do_label_link.id = release_label.label
    JOIN release ON release_label.release = release.id
    JOIN release_name ON release.name = release_name.id
    JOIN medium ON medium.release = release.id
    JOIN tracklist ON medium.tracklist = tracklist.id
    WHERE release_label.catalog_number NOTNULL
)
SELECT label_name, label_gid, catalog_number, gid, name, SUM(track_count) AS track_count, format, country, year, status
INTO do_release_link_catno
FROM r
GROUP BY label_name, label_gid, catalog_number, gid, name, format, country, year, status;

DELETE FROM do_release_link_catno USING do_release_link WHERE do_release_link_catno.gid = do_release_link.gid;
"""
        mbquery_cleanup = "DROP TABLE IF EXISTS do_release_link_catno"
        doquery = """
DROP TABLE IF EXISTS rel_catno;

WITH mb_release_link_catno AS (
    SELECT DISTINCT t1.* FROM dblink(:dblink, 'SELECT label_gid, label_name, catalog_number, gid, name, track_count, format, country, year, status FROM do_release_link_catno')
    AS t1(label_gid uuid, label_name text, catalog_number text, gid uuid, name text, track_count integer, format integer, country integer, year smallint, status integer)
)
SELECT DISTINCT mb_label_link.id AS label_id, mb_release_link_catno.*, releases_labels.release_id
INTO rel_catno 
FROM mb_release_link_catno
JOIN mb_label_link ON mb_release_link_catno.label_gid = mb_label_link.gid
JOIN label ON mb_label_link.id = label.id
JOIN releases_labels ON label.name = releases_labels.label
WHERE regexp_replace(releases_labels.catno, ' ', '') = regexp_replace(mb_release_link_catno.catalog_number, ' ', '');

DROP TABLE IF EXISTS discogs_db_release_link;
WITH rel_catno2 AS (
    SELECT DISTINCT rel_catno.* FROM rel_catno
    JOIN release_mb_mapping ON rel_catno.release_id = release_mb_mapping.release_id
    WHERE upper(release_mb_mapping.title) = upper(rel_catno.name)
    AND release_mb_mapping.track_count = rel_catno.track_count
    AND (rel_catno.format = release_mb_mapping.format_id 
        OR (rel_catno.format = ANY(array[7, 29, 30, 31]) AND release_mb_mapping.format_id = ANY(array[7, 29, 30, 31])))
    AND rel_catno.country = release_mb_mapping.country_id
    AND release_mb_mapping.year = rel_catno.year
    AND release_mb_mapping.status = rel_catno.status
)
SELECT gid, 'http://www.discogs.com/release/' || release_id AS url, 'Release found on linked Discogs label with same normalized catalog number,'
        || ' case insensitive match on release name, same number of tracks, same format, same release country, and same release year' AS note
INTO discogs_db_release_link
FROM rel_catno2
ORDER BY label_name, gid;

DELETE FROM discogs_db_release_link
WHERE gid IN (
  SELECT gid FROM discogs_db_release_link
  GROUP BY gid
  HAVING (COUNT(gid) > 1)
);

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
) AND artist_name = substring(discogs_artist_name FROM '(.+?)(?: \(\d+\))?$');

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

    def artist_link_table2(self):
        self.open(do=True, mb=True)
        mbquery = """
DROP TABLE IF EXISTS do_recordings;
WITH recordings AS (
    SELECT DISTINCT do_release_link.url, do_release_link.name AS release_name, do_release_link.gid as release_gid, 
        track_name.name AS track_name, recording.gid AS recording_gid, artist_name.name AS artist_name, artist.gid AS artist_gid 
    FROM do_release_link
    JOIN release ON release.id = do_release_link.id 
    JOIN medium ON medium.release = release.id
    JOIN tracklist ON medium.tracklist = tracklist.id
    JOIN track ON track.tracklist = tracklist.id
    JOIN recording ON track.recording = recording.id
    JOIN artist_credit ON recording.artist_credit = artist_credit.id
    JOIN artist_credit_name ON artist_credit_name.artist_credit = artist_credit.id
    JOIN artist_name ON artist_credit_name.name = artist_name.id
    JOIN artist ON artist_credit_name.artist = artist.id
    JOIN track_name ON recording.name = track_name.id
    WHERE release.artist_credit = 1
    AND artist_credit.artist_count = 1
)
SELECT * 
INTO do_recordings
FROM recordings
WHERE artist_gid NOT IN (SELECT gid FROM do_artist_link);
"""
        doquery = """
WITH mb_recordings2 AS (
    WITH mb_recordings AS (
        SELECT DISTINCT decodeURL(t1.url, 'http://www.discogs.com/release/') AS release_id, t1.*  
        FROM dblink(:dblink, 'SELECT url, release_name, release_gid, track_name, recording_gid, artist_name, artist_gid FROM do_recordings')
        AS t1(url text, release_name text, release_gid uuid, track_name text, recording_gid uuid, artist_name text, artist_gid uuid)
    )
    SELECT DISTINCT mb_recordings.artist_name, artist_gid, 'Recording: ' || track_name 
    || ' by ' || mb_recordings.artist_name || ' in release "' || release_name 
    || '": http://musicbrainz.org/release/' || release_gid || 
    ' is linked to Discogs release ' || url || ' Track credits points to same artist' AS note
    FROM mb_recordings
    JOIN track ON mb_recordings.release_id = track.release_id::text
    JOIN tracks_artists ON tracks_artists.track_id = track.track_id
    WHERE mb_recordings.track_name = track.title
    AND mb_recordings.artist_name = tracks_artists.artist_name
), mb_recordings3 AS (
    SELECT artist_gid, 'http://www.discogs.com/artist/' || url (artist_name) AS url, 'Artist "' 
    || artist_name || '" with exact match on name found on Discogs.
    ' || string_agg(note, ',
    ') AS releases
    FROM mb_recordings2
    GROUP BY artist_gid, artist_name
)
INSERT INTO discogs_db_artist_link2 (artist_gid, url, releases)
SELECT artist_gid, url, releases FROM mb_recordings3
EXCEPT
SELECT artist_gid, url, releases FROM discogs_db_artist_link
ORDER BY url;
"""
        self.mbdb.execute(mbquery)
        print 'MB side done!'
        self.dodb.execute(text(doquery), dblink=cfg.MB_DB_LINK)
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
