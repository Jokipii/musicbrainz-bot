import mechanize
import urllib
import time
import re
from mbbot.guesscase import guess_artist_sort_name


def format_time(secs):
    return '%0d:%02d' % (secs / 60, secs % 60)


def album_to_form(album):
    form = {}
    form['artist_credit.names.0.artist.name'] = album['artist']
    form['artist_credit.names.0.name'] = album['artist']
    if album.get('artist_mbid'):
        form['artist_credit.names.0.mbid'] = album['artist_mbid']
    form['name'] = album['title']
    if album.get('date'):
        date_parts = album['date'].split('-')
        if len(date_parts) > 0:
            form['date.year'] = date_parts[0]
            if len(date_parts) > 1:
                form['date.month'] = date_parts[1]
                if len(date_parts) > 2:
                    form['date.day'] = date_parts[2]
    if album.get('label'):
        form['labels.0.name'] = album['label']
    if album.get('barcode'):
        form['barcode'] = album['barcode']
    for medium_no, medium in enumerate(album['mediums']):
        form['mediums.%d.format' % medium_no] = medium['format']
        form['mediums.%d.position' % medium_no] = medium['position']
        for track_no, track in enumerate(medium['tracks']):
            form['mediums.%d.track.%d.position' % (medium_no, track_no)] = track['position']
            form['mediums.%d.track.%d.name' % (medium_no, track_no)] = track['title']
            form['mediums.%d.track.%d.length' % (medium_no, track_no)] = format_time(track['length'])
    form['edit_note'] = 'http://www.cdbaby.com/cd/' + album['_id'].split(':')[1]
    return form


def extract_artist_mbid(url):
    m = re.search(r'/artist/([0-9a-f-]{36})$', url)
    if m is None:
        return None
    return m.group(1)


def extract_release_mbid(url):
    m = re.search(r'/release/([0-9a-f-]{36})$', url)
    if m is None:
        return None
    return m.group(1)


class MusicBrainzClient(object):

    def __init__(self, username, password, server="http://musicbrainz.org"):
        self.server = server
        self.username = username
        self.b = mechanize.Browser()
        self.b.set_handle_robots(False)
        self.b.set_debug_redirects(False)
        self.b.set_debug_http(False)
        self.b.addheaders = [('User-agent', 'musicbrainz-bot/1.0 ( %s/user/%s )' % (server, username))]
        self.login(username, password)

    def url(self, path, **kwargs):
        query = ''
        if kwargs:
            query = '?' + urllib.urlencode([(k, v.encode('utf8')) for (k, v) in kwargs.items()])
        return self.server + path + query

    def login(self, username, password):
        self.b.open(self.url("/login"))
        self.b.select_form(predicate=lambda f: f.method == "POST" and "/login" in f.action)
        self.b["username"] = username
        self.b["password"] = password
        self.b["remember_me"] = ["1"]
        self.b.submit()
        resp = self.b.response()
        if resp.geturl() != self.url("/user/" + username):
            raise Exception('unable to login')

    def add_release(self, album, edit_note, auto=False):
        form = album_to_form(album)
        self.b.open(self.url("/release/add"), urllib.urlencode(form))
        time.sleep(2.0)
        self.b.select_form(predicate=lambda f: f.method == "POST" and "/release" in f.action)
        self.b.submit(name="step_editnote")
        time.sleep(2.0)
        self.b.select_form(predicate=lambda f: f.method == "POST" and "/release" in f.action)
        print self.b.response().read()
        self.b.submit(name="save")
        release_mbid = extract_release_mbid(self.b.geturl())
        if not release_mbid:
            raise Exception('unable to post edit')
        return release_mbid

    def add_artist(self, artist, edit_note, auto=False):
        self.b.open(self.url("/artist/create"))
        self.b.select_form(predicate=lambda f: f.method == "POST" and "/artist/create" in f.action)
        self.b["edit-artist.name"] = artist['name']
        self.b["edit-artist.sort_name"] = artist.get('sort_name', guess_artist_sort_name(artist['name']))
        self.b["edit-artist.edit_note"] = edit_note.encode('utf8')
        self.b.submit()
        mbid = extract_artist_mbid(self.b.geturl())
        if not mbid:
            raise Exception('unable to post edit')
        return mbid

    def add_url(self, entity_type, entity_id, link_type_id, url, edit_note='', auto=False):
        self.b.open(self.url("/edit/relationship/create_url", entity=entity_id, type=entity_type))
        self.b.select_form(predicate=lambda f: f.method == "POST" and "create_url" in f.action)
        self.b["ar.link_type_id"] = [str(link_type_id)]
        self.b["ar.url"] = str(url)
        self.b["ar.edit_note"] = edit_note.encode('utf8')
        try: self.b["ar.as_auto_editor"] = ["1"] if auto else []
        except mechanize.ControlNotFoundError: pass
        self.b.submit()
        page = self.b.response().read()
        if "Thank you, your edit has been" not in page:
            if "already exists" not in page:
                raise Exception('unable to post edit')

    def edit_artist(self, artist, update, edit_note, auto=False):
        self.b.open(self.url("/artist/%s/edit" % (artist['gid'],)))
        self.b.select_form(predicate=lambda f: f.method == "POST" and "/edit" in f.action)
        if 'country' in update:
            if self.b["edit-artist.country_id"] != ['']:
                print " * country already set, not changing"
                return
            self.b["edit-artist.country_id"] = [str(artist['country'])]
        if 'type' in update:
            if self.b["edit-artist.type_id"] != ['']:
                print " * type already set, not changing"
                return
            self.b["edit-artist.type_id"] = [str(artist['type'])]
        if 'gender' in update:
            if self.b["edit-artist.gender_id"] != ['']:
                print " * gender already set, not changing"
                return
            self.b["edit-artist.gender_id"] = [str(artist['gender'])]
        if 'begin_date' in update:
            if self.b["edit-artist.begin_date.year"]:
                print " * begin date year already set, not changing"
                return
            self.b["edit-artist.begin_date.year"] = str(artist['begin_date_year'])
            if artist['begin_date_month']:
                self.b["edit-artist.begin_date.month"] = str(artist['begin_date_month'])
                if artist['begin_date_day']:
                    self.b["edit-artist.begin_date.day"] = str(artist['begin_date_day'])
        if 'end_date' in update:
            if self.b["edit-artist.end_date.year"]:
                print " * end date year already set, not changing"
                return
            self.b["edit-artist.end_date.year"] = str(artist['end_date_year'])
            if artist['end_date_month']:
                self.b["edit-artist.end_date.month"] = str(artist['end_date_month'])
                if artist['end_date_day']:
                    self.b["edit-artist.end_date.day"] = str(artist['end_date_day'])
        if 'comment' in update:
            if self.b["edit-artist.comment"] != '':
                print " * comment already set, not changing"
                return
            self.b["edit-artist.comment"] = artist['comment'].encode('utf-8')
        self.b["edit-artist.edit_note"] = edit_note.encode('utf8')
        try: self.b["edit-artist.as_auto_editor"] = ["1"] if auto else []
        except mechanize.ControlNotFoundError: pass
        self.b.submit()
        page = self.b.response().read()
        if "Thank you, your edit has been" not in page:
            if 'any changes to the data already present' not in page:
                raise Exception('unable to post edit')

    def set_artist_type(self, entity_id, type_id, edit_note, auto=False):
        self.b.open(self.url("/artist/%s/edit" % (entity_id,)))
        self.b.select_form(predicate=lambda f: f.method == "POST" and "/edit" in f.action)
        if self.b["edit-artist.type_id"] != ['']:
            print " * already set, not changing"
            return
        self.b["edit-artist.type_id"] = [str(type_id)]
        self.b["edit-artist.edit_note"] = edit_note.encode('utf8')
        try: self.b["edit-artist.as_auto_editor"] = ["1"] if auto else []
        except mechanize.ControlNotFoundError: pass
        self.b.submit()
        page = self.b.response().read()
        if "Thank you, your edit has been" not in page:
            if 'any changes to the data already present' not in page:
                raise Exception('unable to post edit')

    def set_artist_country_id(self, entity_id, country_id, edit_note, auto=False):
        self.b.open(self.url("/artist/%s/edit" % (entity_id,)))
        self.b.select_form(predicate=lambda f: f.method == "POST" and "/edit" in f.action)
        if self.b["edit-artist.country_id"] != ['']:
            print " * already set, not changing"
            return
        self.b["edit-artist.country_id"] = [str(country_id)]
        self.b["edit-artist.edit_note"] = edit_note.encode('utf8')
        try: self.b["edit-artist.as_auto_editor"] = ["1"] if auto else []
        except mechanize.ControlNotFoundError: pass
        self.b.submit()
        page = self.b.response().read()
        if "Thank you, your edit has been" not in page:
            if 'any changes to the data already present' not in page:
                raise Exception('unable to post edit')

    def edit_url(self, entity_id, old_url, new_url, edit_note, auto=False):
        self.b.open(self.url("/url/%s/edit" % (entity_id,)))
        self.b.select_form(predicate=lambda f: f.method == "POST" and "/edit" in f.action)
        if self.b["edit-url.url"] != str(old_url):
            print " * value has changed, aborting"
            return
        if self.b["edit-url.url"] == str(new_url):
            print " * already set, not changing"
            return
        self.b["edit-url.url"] = str(new_url)
        self.b["edit-url.edit_note"] = edit_note.encode('utf8')
        try: self.b["edit-url.as_auto_editor"] = ["1"] if auto else []
        except mechanize.ControlNotFoundError: pass
        self.b.submit()
        page = self.b.response().read()
        if "Thank you, your edit has been" not in page:
            if "any changes to the data already present" not in page:
                raise Exception('unable to post edit')

    def add_relationship(self, entity0_type, entity1_type, entity0_gid, entity1_gid, link_type_id, attributes, edit_note, auto=False):
        self.b.open(self.url("/edit/relationship/create", type0=entity0_type, type1=entity1_type, entity0=entity0_gid, entity1=entity1_gid))
        self.b.select_form(predicate=lambda f: f.method == "POST" and "/edit" in f.action)
        self.b["ar.link_type_id"] = [str(link_type_id)]
        for k, v in attributes.items():
            self.b["ar.attrs."+k] = v
        self.b["ar.edit_note"] = edit_note.encode('utf8')
        try: self.b["ar.as_auto_editor"] = ["1"] if auto else []
        except mechanize.ControlNotFoundError: pass
        self.b.submit()
        page = self.b.response().read()
        if "Thank you, your edit has been" not in page:
            if "exists with these attributes" not in page:
                raise Exception('unable to post edit')

    def edit_relationship(self, rel_id, entity0_type, entity1_type, old_link_type_id, new_link_type_id, attributes, edit_note, auto=False):
        self.b.open(self.url("/edit/relationship/edit", id=str(rel_id), type0=entity0_type, type1=entity1_type))
        self.b.select_form(predicate=lambda f: f.method == "POST" and "/edit" in f.action)
        if self.b["ar.link_type_id"] == [str(new_link_type_id)] and new_link_type_id != old_link_type_id:
            print " * already set, not changing"
            return
        if self.b["ar.link_type_id"] != [str(old_link_type_id)]:
            print " * value has changed, aborting"
            return
        self.b["ar.link_type_id"] = [str(new_link_type_id)]
        for k, v in attributes.items():
            self.b["ar.attrs."+k] = v
        #for k, v in begin_date.items():
        #    self.b["ar.begin_date."+k] = str(v)
        #for k, v in end_date.items():
        #    self.b["ar.end_date."+k] = str(v)
        self.b["ar.edit_note"] = edit_note.encode('utf8')
        try: self.b["ar.as_auto_editor"] = ["1"] if auto else []
        except mechanize.ControlNotFoundError: pass
        self.b.submit()
        page = self.b.response().read()
        if "Thank you, your edit has been" not in page:
            if "exists with these attributes" not in page:
                raise Exception('unable to post edit')

    def remove_relationship(self, rel_id, entity0_type, entity1_type, edit_note):
        self.b.open(self.url("/edit/relationship/delete", id=str(rel_id), type0=entity0_type, type1=entity1_type))
        self.b.select_form(predicate=lambda f: f.method == "POST" and "/edit" in f.action)
        self.b["confirm.edit_note"] = edit_note.encode('utf8')
        self.b.submit()
        page = self.b.response().read()
        if "Thank you, your edit has been" not in page:
            raise Exception('unable to post edit')

    def merge(self, entity_type, entity_ids, target_id, edit_note):
        params = [('add-to-merge', id) for id in entity_ids]
        self.b.open(self.url("/%s/merge_queue" % entity_type), urllib.urlencode(params))
        page = self.b.response().read()
        if "You are about to merge" not in page:
            raise Exception('unable to add items to merge queue')

        params = {'merge.target': target_id, 'submit': 'submit', 'merge.edit_note': edit_note}
        for idx, val in enumerate(entity_ids):
            params['merge.merging.%s' % idx] = val
        self.b.open(self.url("/%s/merge" % entity_type), urllib.urlencode(params))
        page = self.b.response().read()
        if "Thank you, your edit has been" not in page:
            raise Exception('unable to post edit')

    def _edit_release_information(self, entity_id, attributes, edit_note, auto=False):
        self.b.open(self.url("/release/%s/edit" % (entity_id,)))
        self.b.select_form(predicate=lambda f: f.method == "POST" and "/edit" in f.action)
        changed = False
        for k, v in attributes.items():
            self.b.form.find_control(k).readonly = False
            if self.b[k] != v[0]:
                print " * %s has changed, aborting" % k
                return
            if self.b[k] != v[1]:
                changed = True
                self.b[k] = v[1]
        if not changed:
            print " * already set, not changing"
            return
        self.b["barcode_confirm"] = ["1"]
        self.b.submit(name="step_editnote")
        page = self.b.response().read()
        self.b.select_form(predicate=lambda f: f.method == "POST" and "/edit" in f.action)
        try:
            self.b["edit_note"] = edit_note.encode('utf8')
        except mechanize.ControlNotFoundError:
            raise Exception('unable to post edit')
        try: self.b["as_auto_editor"] = ["1"] if auto else []
        except mechanize.ControlNotFoundError: pass
        self.b.submit(name="save")
        page = self.b.response().read()
        if "Release information" not in page:
            raise Exception('unable to post edit')

    def set_release_barcode(self, entity_id, old_barcode, new_barcode, edit_note, auto=False):
        self._edit_release_information(entity_id, {"barcode": [str(old_barcode),str(new_barcode)]}, edit_note, auto)

    def set_release_script(self, entity_id, old_script_id, new_script_id, edit_note, auto=False):
        self._edit_release_information(entity_id, {"script_id": [[str(old_script_id)],[str(new_script_id)]]}, edit_note, auto)

    def set_release_language(self, entity_id, old_language_id, new_language_id, edit_note, auto=False):
        self._edit_release_information(entity_id, {"language_id": [[str(old_language_id)],[str(new_language_id)]]}, edit_note, auto)

    def set_release_medium_format(self, entity_id, old_format_id, new_format_id, edit_note, auto=False):
        self.b.open(self.url("/release/%s/edit" % (entity_id,)))

        self.b.select_form(predicate=lambda f: f.method == "POST" and "/edit" in f.action)
        self.b["barcode_confirm"] = ["1"]
        self.b.submit(name="step_tracklist")

        self.b.select_form(predicate=lambda f: f.method == "POST" and "/edit" in f.action)
        attributes = {"mediums.0.format_id": [[str(old_format_id)], [str(new_format_id)]]}
        changed = False
        for k, v in attributes.items():
            if self.b[k] != v[0]:
                print " * %s has changed, aborting" % k
                return
            if self.b[k] != v[1]:
                changed = True
                self.b[k] = v[1]
        if not changed:
            print " * already set, not changing"
            return
        self.b.submit(name="step_editnote")
        page = self.b.response().read()
        self.b.select_form(predicate=lambda f: f.method == "POST" and "/edit" in f.action)
        try:
            self.b["edit_note"] = edit_note.encode('utf8')
        except mechanize.ControlNotFoundError:
            raise Exception('unable to post edit')
        #try: self.b["as_auto_editor"] = ["1"] if auto else []
        #except mechanize.ControlNotFoundError: pass
        self.b.submit(name="save")
        page = self.b.response().read()
        if "Release information" not in page:
            raise Exception('unable to post edit')

    def add_edit_note(self, identify, edit_note):
        '''Adds an edit note to the last (or very recently) made edit. This
        is necessary e.g. for ISRC submission via web service, as it has no
        support for edit notes. The "identify" argument is a function
            function(str, str) -> bool
        which receives the edit number as first, the raw html body of the edit
        as second argument, and determines if the note should be added to this
        edit.'''
        self.b.open(self.url("/user/%s/edits" % (self.username,)))
        page = self.b.response().read()
        self.b.select_form(predicate=lambda f: f.method == "POST" and "/edit" in f.action)
        edits = re.findall(r'<h2><a href="'+self.server+r'/edit/([0-9]+).*?<div class="edit-details">(.*?)</div>', page, re.S)
        for i, (edit_nr, text) in enumerate(edits):
            if identify(edit_nr, text):
                self.b['enter-vote.vote.%d.edit_note' % i] = edit_note.encode('utf8')
                break
        self.b.submit()

    def edit_work(self, entity_id, update, edit_note, auto=False):
        self.b.open(self.url("/work/%s/edit" % (entity_id,)))
        self.b.select_form(predicate=lambda f: f.method == "POST" and "/edit" in f.action)
        # rest of fields can be added when someone needs those
        if 'iswc' in update:
            if self.b["edit-work.iswc"] != '':
                print " * ISWC already set, not changing"
                return
            self.b["edit-work.iswc"] = str(update['iswc'])
        self.b["edit-work.edit_note"] = edit_note.encode('utf8')
        try: self.b["edit-work.as_auto_editor"] = ["1"] if auto else []
        except mechanize.ControlNotFoundError: pass
        self.b.submit()
        page = self.b.response().read()
        if "Thank you, your edit has been" not in page:
            if 'any changes to the data already present' not in page:
                raise Exception('unable to post edit')

    def cancel_edit(self, edit_nr, edit_note=u''):
        self.b.open(self.url("/edit/%s/cancel" % (edit_nr,)))
        page = self.b.response().read()
        self.b.select_form(predicate=lambda f: f.method == "POST" and "/cancel" in f.action)
        if edit_note:
            self.b['confirm.edit_note'] = edit_note.encode('utf8')
        self.b.submit()
