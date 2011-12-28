from BeautifulSoup import BeautifulSoup
import mechanize
import urllib
import re
import config as cfg
import sqlalchemy
import socket

class AscapClient(object):

    def __init__(self):
        self.server = 'http://www.ascap.com'
        self.b = mechanize.Browser()
        self.b.set_handle_robots(False)
        self.b.set_debug_redirects(False)
        self.b.set_debug_http(False)
        self.mbengine = sqlalchemy.create_engine(cfg.MB_DB)
        self.mbdb = self.mbengine.connect()
        self.init_database()

    def do_works(self):
        # go thru all works that are not yet done
        query = 'SELECT id FROM ascap_work WHERE done = false'
        result = self.mbdb.execute(query).fetchall()
        for row in result:
            id = str(row[0])
            try:
                if not (self.work_is_done(id)):
                    self.get_work_info(id)
            except AttributeError, msg:
                print 'AttributeException...' + msg
            except socket.error, msg:
                print 'socket.error...' + msg

    def search_writer(self, searchstr):
        page = self.search(searchstr, ['w'])
        # get all ids, names and ipicodes from page
        soup = BeautifulSoup(page)
        links = soup.find('table', width="490").findAll('a')
        ids, names = self.parse_artists(links)
        links = soup.find('table', width="490").findAll(text=re.compile('\d{5,11}'))
        ipicodes = list()
        for link in links:
            ipicodes.append(link.strip())
        # now we have all ids, names and ipicodes in lists
        for i in range(len(ids)):
            self.store_artist(ids[i], names[i], ipicodes[i])
        return ids, names, ipicodes

    def search_title(self, searchstr):
        page = self.search(searchstr, ['t'])
        # TODO

    def search_performer(self, searchstr):
        page = self.search(searchstr, ['p'])
        # TODO

    def search_iswc(self, searchstr):
        page = self.search(searchstr, ['x'])
        # TODO

    def get_artist_works(self, artistid):
        res = self.b.open(self.search_url(artistid, 'c'))
        soup = BeautifulSoup(res.read())
        res.close()
        # get all work ids and names
        works = soup.findAll(text=re.compile('Work ID'))
        for work in works:
            workid = re.search('\d+', work).group(0)
            name = work.parent.parent.parent.parent.find('a').string
            # store initial work and writer
            self.store_work(workid, name)
            self.store_writer(artistid, workid)
        self.artist_done(artistid)

    def get_work_info(self, workid):
        res = self.b.open(self.search_url(workid, 'i'))
        soup = BeautifulSoup(res.read())
        res.close()
        # get work name and iswc
        name = soup.find(text=re.compile('Work ID:')).parent.parent.parent.parent.findAll('b')[1].string.strip()
        try:
            iswc = self.format_iswc(re.search('T\d+', soup.find(text=re.compile('ISWC:'))).group(0))
        except (TypeError):
            # this exception comes when work din't contain ISWC code
            iswc = None
        # store work
        self.store_work(workid, name, iswc)
        self.work_done(workid)
        # get all writers
        writers = soup.find(text=re.compile('Writers:'))
        if (writers != None):
            writers = writers.parent.parent.parent.findAll('a')
            self.parse_artists(writers, True)
        if (iswc != None):
            print iswc + ' ' + name

    def parse_artists(self, links, store=False):
        ids = list()
        names = list()
        for link in links:
            part = link['href'][53:]
            id = part[0:part.find('&')]
            name = link.string.strip()
            ids.append(id)
            names.append(name)
            if (store):
                self.store_artist(id, name)
        return ids, names

    def format_iswc(self, iswc):
        return iswc[0] + '-' + iswc[1:4] + '.' + iswc[4:7] + '.' + iswc[7:10] + '-' + iswc[10]

    def search_url(self, searchstr, search_in):
        path = '/ace/search.cfm'
        query1 = '?requesttimeout=300&mode=results&searchstr='
        query2 = '&search_in='
        query3 = '&search_type=exact&search_det=t,s,w,p,b,v&results_pp=25&start=1'
        return self.server + path + query1 + searchstr + query2 + search_in + query3

    def search(self, searchstr, search_in):
        self.b.open(self.url('/ace/search.cfm?mode=search'))
        self.b.select_form(name='CFForm_1')
        self.b['searchstr'] = searchstr
        self.b['search_in'] = search_in
        self.b.submit()
        page = self.b.response().read()
        self.b.response().close()
        return page

    def url(self, path, **kwargs):
        query = ''
        if kwargs:
            query = '?' + urllib.urlencode([(k, v.encode('utf8')) for (k, v) in kwargs.items()])
        return self.server + path + query

    def get_mb_data(self):
        query = """
        SELECT * FROM ascap_mb
        """
        return self.mbdb.execute(query).fetchall()
        return [("e8a5fe99-0aff-3e7b-840c-7ab96cb702be","320231156","T-070.015.405-7","Bruce Springsteen","Badlands"),\
        ("6ce583b4-ca66-3f52-a7e8-5941f7f49ae0","320445970","T-070.019.549-8","Bruce Springsteen","Balboa Park")]

    def artist_is_done(self, id):
        query = 'SELECT done FROM ascap_artist_id WHERE id = %s'
        result = self.mbdb.execute(query, id).fetchone()
        return result[0]

    def work_is_done(self, id):
        query = 'SELECT done FROM ascap_work WHERE id = %s'
        result = self.mbdb.execute(query, id).fetchone()
        return result[0]

    def artist_done(self, id):
        query = 'UPDATE ascap_artist_id SET done = true WHERE id = %s'
        self.mbdb.execute(query, id)

    def work_done(self, id):
        query = 'UPDATE ascap_work SET done = true WHERE id = %s'
        self.mbdb.execute(query, id)

    def store_writer(self, artist_id, work_id):
        query = 'INSERT INTO ascap_writer (artist, work) VALUES (%s,%s)'
        self.mbdb.execute(query, artist_id, work_id)

    def store_artist(self, id, name, ipi = None):
        query1 = 'INSERT INTO ascap_artist_id (id) VALUES (%s)'
        query2 = 'INSERT INTO ascap_artist (artist, name, ipicode) VALUES (%s,%s,%s)'
        if (self.mbdb.execute('SELECT id FROM ascap_artist_id WHERE id = %s', id).rowcount == 0):
            self.mbdb.execute(query1, id)
        self.mbdb.execute(query2, id, name, ipi)

    def store_work(self, id, name, iswc = None):
        query = 'INSERT INTO ascap_work (id, name, iswc) VALUES (%s,%s,%s)'
        self.mbdb.execute(query, id, name, iswc)

    def init_database(self):
        query = """
        CREATE TABLE IF NOT EXISTS ascap_artist_id (
            id integer PRIMARY KEY,
            done boolean DEFAULT false);

        CREATE TABLE IF NOT EXISTS ascap_artist (
            id serial PRIMARY KEY,
            artist integer REFERENCES ascap_artist_id (id),
            name varchar(60),
            ipicode varchar(11));
        CREATE OR REPLACE RULE ascap_artist_insert_rule AS ON INSERT TO ascap_artist
            WHERE (new.artist, new.name) IN 
            (SELECT artist, name FROM ascap_artist WHERE artist = new.artist AND name = new.name)
            DO INSTEAD
            UPDATE ascap_artist SET ipicode = new.ipicode WHERE artist = new.artist AND name = new.name;

        CREATE TABLE IF NOT EXISTS ascap_work (
            id integer PRIMARY KEY,
            iswc character(15),
            name varchar(60),
            done boolean DEFAULT false);
        CREATE OR REPLACE RULE ascap_work_insert_rule AS ON INSERT TO ascap_work
            WHERE new.id IN (SELECT id FROM ascap_work WHERE id = new.id)
            DO INSTEAD
            UPDATE ascap_work SET iswc = new.iswc, name = new.name WHERE id = new.id;

        CREATE TABLE IF NOT EXISTS ascap_writer (
            id serial PRIMARY KEY,
            artist integer REFERENCES ascap_artist_id (id),
            work integer REFERENCES ascap_work (id));
        CREATE OR REPLACE RULE ascap_writer_insert_rule AS ON INSERT TO ascap_writer
            WHERE (new.artist, new.work) IN 
            (SELECT artist, work FROM ascap_writer WHERE artist = new.artist AND work = new.work)
            DO NOTHING;
        """
        self.mbdb.execute(query)
