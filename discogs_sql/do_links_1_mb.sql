DROP INDEX do_artist_link_idx_url;
DROP INDEX do_artist_link_idx_gid;
SELECT refresh_matview('do_artist_link');
CREATE INDEX do_artist_link_idx_gid ON do_artist_link(gid);
CREATE INDEX do_artist_link_idx_url ON do_artist_link(url);
ANALYZE do_artist_link;

DROP INDEX do_release_link_idx_url;
DROP INDEX do_release_link_idx_gid;
SELECT refresh_matview('do_release_link');
CREATE INDEX do_release_link_idx_gid ON do_release_link(gid);
CREATE INDEX do_release_link_idx_url ON do_release_link(url);
ANALYZE do_release_link;
