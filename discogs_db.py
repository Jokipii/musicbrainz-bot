import config as cfg
import sqlalchemy
from sqlalchemy.sql import text
from editing import MusicBrainzClient
from datetime import date
import httplib

def get_status_code(host, path="/"):
    """ This function retreives the status code of a website by requesting
        HEAD data from the host. This means that it only requests the headers.
        If the host cannot be reached or something else goes wrong, it returns
        None instead.
    """
    try:
        conn = httplib.HTTPConnection(host)
        conn.request("HEAD", path)
        return conn.getresponse().status
    except StandardError:
        return None

def read_query(name):
    with open('discogs_sql/'+name+'.sql','r') as file:
        query = file.read()
        file.close()
    return query

class DiscogsDbClient(object):
    
    def __init__(self):
        self.mbengine = sqlalchemy.create_engine(cfg.MB_DB)
        self.doengine = sqlalchemy.create_engine(cfg.DO_DB)

    def commit_artist_all(self, limit):
        mbClient = self.open(do=True, client=True)
        queryLinks = "SELECT gid, url, note FROM discogs_db_artist_all LIMIT %s"
        queryDelete = "DELETE FROM discogs_db_artist_all WHERE gid = %s"
        for gid, url, note in self.dodb.execute(queryLinks, limit):
            mbClient.add_url("artist", gid, 180, "http://www.discogs.com/artist/" + url, note)
            self.dodb.execute(queryDelete, gid)
            print url + " Done!"
        self.close()

    def commit_artist_all2(self, limit):
        mbClient = self.open(do=True, client=True)
        queryLinks = "SELECT gid, url, note FROM discogs_db_artist_all2 LIMIT %s"
        queryDelete = "DELETE FROM discogs_db_artist_all2 WHERE gid = %s"
        for gid, url, note in self.dodb.execute(queryLinks, limit):
            mbClient.add_url("artist", gid, 180, "http://www.discogs.com/artist/" + url, note)
            self.dodb.execute(queryDelete, gid)
            print url + " Done!"
        self.close()

    def commit_member_of_band(self, limit):
        mbClient = self.open(do=True, client=True)
        queryLinks = "SELECT gid0, gid1, note FROM discogs_db_member_of_band LIMIT %s"
        queryDelete = "DELETE FROM discogs_db_member_of_band WHERE gid0 = :gid0 AND gid1 = :gid1"
        for gid0, gid1, note in self.dodb.execute(queryLinks, limit):
            mbClient.add_relationship("artist", "artist", gid0, gid1, 103, {}, note)
            self.dodb.execute(text(queryDelete), gid0=gid0, gid1=gid1)
            print note + " Done!"
        self.close()

    def commit_perform_as(self, limit):
        mbClient = self.open(do=True, client=True)
        queryLinks = "SELECT gid0, gid1, note FROM discogs_db_perform_as LIMIT %s"
        queryDelete = "DELETE FROM discogs_db_perform_as WHERE gid0 = :gid0 AND gid1 = :gid1"
        for gid0, gid1, note in self.dodb.execute(queryLinks, limit):
            mbClient.add_relationship("artist", "artist", gid0, gid1, 108, {}, note)
            self.dodb.execute(text(queryDelete), gid0=gid0, gid1=gid1)
            print note + " Done!"
        self.close()

    def commit_label_links(self, limit):
        mbClient = self.open(do=True, client=True)
        queryLinks = "SELECT gid, url, note FROM discogs_db_label_link LIMIT %s"
        queryDelete = "DELETE FROM discogs_db_label_link WHERE gid = %s"
        results = self.dodb.execute(queryLinks, limit).fetchall()
        for gid, url, note in results:
            mbClient.add_url("label", gid, 217, 'http://www.discogs.com/label/'+url, note)
            self.dodb.execute(queryDelete, gid)
            print url + " Done!"
        self.close()

    def commit_release_links(self, limit):
        mbClient = self.open(do=True, client=True)
        queryLinks = "SELECT gid, url, note FROM discogs_db_release_link ORDER BY note LIMIT %s"
        queryDelete = "DELETE FROM discogs_db_release_link WHERE gid = %s"
        results = self.dodb.execute(queryLinks, limit).fetchall()
        for gid, url, note in results:
            mbClient.add_url("release", gid, 76, url, note)
            self.dodb.execute(queryDelete, gid)
            print url + " Done!"
        self.close()

    def commit_release_format(self, limit):
        mbClient = self.open(do=True, client=True)
        queryLinks = "SELECT gid, format_id, url FROM discogs_db_release_format LIMIT %s"
        queryDelete = "DELETE FROM discogs_db_release_format WHERE gid = %s"
        results = self.dodb.execute(queryLinks, limit).fetchall()
        for gid, format_id, url in results:
            mbClient.set_release_medium_format(gid, '', format_id, "Format taken from "+url)
            self.dodb.execute(queryDelete, gid)
            print gid + " Done!"
        self.close()

    def commit_release_barcode(self, limit):
        mbClient = self.open(do=True, client=True)
        queryLinks = "SELECT gid, value, url FROM discogs_db_release_barcode LIMIT %s"
        queryDelete = "DELETE FROM discogs_db_release_barcode WHERE gid = %s"
        results = self.dodb.execute(queryLinks, limit).fetchall()
        for gid, value, url in results:
            mbClient.set_release_barcode(gid, '', value, "Barcode from attached Discogs link "+url)
            self.dodb.execute(queryDelete, gid)
            print gid + " Done!"
        self.close()

    def commit_release_cleanup(self, limit):
        mbClient = self.open(do=True, client=True)
        queryLinks = "SELECT DISTINCT id, note FROM discogs_db_release_cleanup LIMIT %s"
        queryDelete = "DELETE FROM discogs_db_release_cleanup WHERE id = %s"
        results = self.dodb.execute(queryLinks, limit).fetchall()
        for id, note in results:
            mbClient.remove_relationship(id, 'release', 'url', note)
            self.dodb.execute(queryDelete, id)
            print str(id) + " Done!"
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

    def commit_recording_credits(self, limit):
        mbClient = self.open(mb=True, client=True)
        queryLinks = "SELECT artist_gid, gid, note FROM remix_temp LIMIT %s"
        queryDelete = "DELETE FROM remix_temp WHERE artist_gid = %s AND gid = %s"
        for artist_gid, gid, note in self.mbdb.execute(queryLinks, limit):
            mbClient.add_relationship("artist", "recording", artist_gid, gid, 153, {}, note)
            self.mbdb.execute(queryDelete, artist_gid, gid)
            print artist_gid + " " + gid + " Done!"
        self.close()

    def run_artist_404(self, limit):
        mbClient = self.open(do=True, client=True)
        doquery = read_query('run_artist_404')
        for id, gid, url in self.dodb.execute(text(doquery), dblink=cfg.MB_DB_LINK, lim=limit):
            if get_status_code("www.discogs.com", url[22:]) == 404:
                mbClient.remove_relationship(id, 'artist', 'url', 'Remove dead link that not found on Discogs (404 - Page not found).')
                print gid+' removed '+url
            else:
                print url+' found, do nothing!'
        self.close()

    def run_label_404(self, limit):
        mbClient = self.open(do=True, client=True)
        doquery = read_query('run_label_404')
        for id, gid, url in self.dodb.execute(text(doquery), dblink=cfg.MB_DB_LINK, lim=limit):
            if get_status_code("www.discogs.com", url[22:]) == 404:
                mbClient.remove_relationship(id, 'label', 'url', 'Remove dead link that not found on Discogs (404 - Page not found).')
                print gid+' removed '+url
            else:
                print url+' found, do nothing!'
        self.close()

    def run_convert_db_relations(self, limit):
        mbClient = self.open(mb=True, client=True)
        query = read_query('run_convert_db_relations')
        for id, link_type in self.mbdb.execute(query, limit):
            mbClient.edit_relationship(id, 'artist', 'url', link_type, 188, {}, 'Convert whitelisted relation to "has a page in a database at"-form', True)
        self.close()

    def run_artist_types(self, limit):
        mbClient = self.open(mb=True, client=True)
        query1 = read_query('run_artist_types_1')
        self.run_artist_type(query1, limit, 1, mbClient)
        query2 = read_query('run_artist_types_2')
        self.run_artist_type(query2, limit, 2, mbClient)
        self.close()

    def run_artist_type(self, query, limit, type, mbClient):
        for gid, note in self.mbdb.execute(query, limit):
            mbClient.set_artist_type(gid, type, note)
            print gid + " Done!"

    def run_artist_country(self, limit):
        mbClient = self.open(mb=True, client=True)
        query = read_query('run_artist_country')
        results = self.mbdb.execute(query, limit).fetchall()
        self.close()
        for country_id, gid, name, comment in results:
            note = "Based on disambiguation comment"
            mbClient.set_artist_country_id(gid, country_id, note)
            print gid + " Done!"

    def run_artist_country2(self, limit):
        mbClient = self.open(do=True, client=True)
        query = read_query('run_artist_country2')
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

    def report_release_artists(self, id):
        self.open(do=True)
        query = read_query('report_release_artists')
        for note in self.dodb.execute(text(query), release_id=id):
            print note[0]
        self.close()

    def do_links(self):
        self.open(do=True)
        doquery = read_query('do_links')
        self.dodb.execute(text(doquery), dblink=cfg.MB_DB_LINK)
        self.close()

    def clean_release_identifiers(self):
        self.open(do=True)
        doquery = read_query('clean_release_identifiers')
        self.dodb.execute(doquery)

    def clean_artist_identifiers(self):
        self.open(do=True)
        doquery = read_query('clean_artist_identifiers')
        self.dodb.execute(doquery)

    def create_track_count(self):
        self.open(do=True)
        doquery = read_query('create_track_count')
        self.dodb.execute(doquery)

    def create_functions(self):
        self.open(do=True)
        doquery = read_query('create_functions')
        self.dodb.execute(doquery)
        self.close()

    def create_link_views(self):
        self.open(mb=True)
        mbquery = read_query('create_link_views')
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
        query = read_query('create_media_type_mapping_table')
        self.dodb.execute(query)

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
        query = read_query('create_release_status_table')
        self.dodb.execute(query)

    def create_release_mb_mapping_table(self):
        self.open(do=True)
        query = read_query('create_release_mb_mapping_table')
        self.dodb.execute(query)

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
        query = read_query('create_country_mapping_table')
        self.dodb.execute(text(query), dblink=cfg.MB_DB_LINK)

    def create_country_search_table(self):
        self.open(mb=True)
        mbquery = read_query('create_country_search_table_mb')
        self.mbdb.execute(text(mbquery))

    def do_release_link_table(self):
        self.open(do=True, mb=True)
        mbquery = read_query('do_release_links_1_mb')
        self.mbdb.execute(mbquery)
        print 'MB side done!'
        doquery = read_query('do_release_links_2')
        self.dodb.execute(text(doquery), dblink=cfg.MB_DB_LINK)
        mbquery_cleanup = "DROP TABLE IF EXISTS do_release_link_catno"
        self.mbdb.execute(mbquery_cleanup)
        self.close()

    def do_artist_evidence_track(self):
        self.open(do=True)
        doquery = read_query('do_artist_evidence_track')
        self.dodb.execute(text(doquery), dblink=cfg.MB_DB_LINK)
        self.close()

    def do_artist_all(self):
        self.open(do=True)
        doquery = read_query('do_artist_all')
        self.dodb.execute(text(doquery), dblink=cfg.MB_DB_LINK)
        self.close()

    def do_member_of_band_table(self):
        self.open(do=True)
        doquery = read_query('do_member_of_band')
        self.dodb.execute(text(doquery), dblink=cfg.MB_DB_LINK)
        self.close()

    def do_perform_as_table(self):
        self.open(do=True)
        doquery = read_query('do_perform_as')
        self.dodb.execute(text(doquery), dblink=cfg.MB_DB_LINK)
        self.close()

    def do_label_link_table(self):
        self.open(do=True)
        doquery = read_query('do_label_link')
        self.dodb.execute(text(doquery), dblink=cfg.MB_DB_LINK)
        self.close()

    def do_release_format_table(self):
        self.open(do=True)
        doquery = read_query('do_release_format')
        self.dodb.execute(text(doquery), dblink=cfg.MB_DB_LINK)
        self.close()

    def do_release_barcode_table(self):
        self.open(do=True)
        doquery = read_query('do_release_barcode')
        self.dodb.execute(text(doquery), dblink=cfg.MB_DB_LINK)
        self.close()

    def do_release_cleanup_table(self):
        self.open(do=True)
        doquery = read_query('do_release_cleanup')
        self.dodb.execute(text(doquery), dblink=cfg.MB_DB_LINK)
        self.close()

    def do_release_credits_table(self):
        self.open(do=True, mb=True)
        doquery = read_query('do_release_credits_1')
        self.dodb.execute(doquery)
        print 'Discogs side done!'
        mbquery = read_query('do_release_credits_2_mb')
        self.mbdb.execute(text(mbquery), dblink=cfg.DO_DB_LINK)
        self.close()
   
    def do_recording_credits_table(self):
        self.open(do=True, mb=True)
        doquery = read_query('do_recording_credits_1')
        self.dodb.execute(doquery)
        print 'Discogs side done!'
        mbquery = read_query('do_recording_credits_2_mb')
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
