DROP TABLE IF EXISTS mb_release_link, mb_release_group_link, mb_artist_link, mb_label_link;

SELECT t1.gid, t1.name, t1.url, (regexp_matches(t1.url, '(?:^http://www.discogs.com/release/)([0-9]+)'))[1]::integer AS id2, edits_pending
INTO mb_release_link 
FROM dblink(:dblink, 
'SELECT gid, name, url, edits_pending FROM do_release_link WHERE url ~ ''^http://www.discogs.com/release/''')
AS t1(gid uuid, name text, url text, edits_pending integer);

ALTER TABLE mb_release_link ADD COLUMN id integer;
UPDATE mb_release_link SET id = release.id FROM release WHERE id2 = release.id;
DELETE FROM mb_release_link WHERE id IS NULL;
ALTER TABLE mb_release_link DROP COLUMN id2;

SELECT t1.gid, t1.name, t1.url, (regexp_matches(t1.url, '(?:^http://www.discogs.com/master/)([0-9]+)'))[1]::integer AS id2, edits_pending
INTO mb_release_group_link 
FROM dblink(:dblink,
'SELECT gid, name, url, edits_pending FROM do_release_group_link WHERE url ~ ''^http://www.discogs.com/master/''')
AS t1(gid uuid, name text, url text, edits_pending integer);

ALTER TABLE mb_release_group_link ADD COLUMN id integer;
UPDATE mb_release_group_link SET id = master.id FROM master WHERE id2 = master.id;
DELETE FROM mb_release_group_link WHERE id IS NULL;
ALTER TABLE mb_release_group_link DROP COLUMN id2;

SELECT t1.gid, t1.name, t1.url, lower(decodeURL(t1.url, 'http://www.discogs.com/artist/')) AS id2, edits_pending
INTO mb_artist_link 
FROM dblink(:dblink,
'SELECT gid, name, url, edits_pending FROM do_artist_link') AS t1(gid uuid, name text, url text, edits_pending integer);

ALTER TABLE mb_artist_link ADD COLUMN id integer;
UPDATE mb_artist_link SET id = artist.id FROM artist WHERE id2 = lower(artist.name);
DELETE FROM mb_artist_link WHERE id IS NULL;
ALTER TABLE mb_artist_link DROP COLUMN id2;

ALTER TABLE mb_artist_link ADD CONSTRAINT mb_artist_link_fk_artist FOREIGN KEY (id) REFERENCES artist(id);

SELECT t1.gid, t1.name, t1.url, lower(decodeURL(t1.url, 'http://www.discogs.com/label/')) AS id2, edits_pending
INTO mb_label_link 
FROM dblink(:dblink,
'SELECT gid, name, url, edits_pending FROM do_label_link') AS t1(gid uuid, name text, url text, edits_pending integer);

ALTER TABLE mb_label_link ADD COLUMN id integer;
UPDATE mb_label_link SET id = label.id FROM label WHERE id2 = lower(label.name);
DELETE FROM mb_label_link WHERE id IS NULL;
ALTER TABLE mb_label_link DROP COLUMN id2;
