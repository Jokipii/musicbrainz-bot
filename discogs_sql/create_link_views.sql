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
