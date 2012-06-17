DROP TABLE IF EXISTS do_release_artist_credits;

WITH do_rac AS (
    SELECT *
    FROM dblink(:dblink, 'SELECT release_gid, release_url, artist_gid, artist_url, roles FROM discogs_db_release_credits')
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
