INSERT INTO update_log(updated, event) VALUES ('report_images', 'started');

WITH images AS (
	SELECT mb_release_link.gid, mb_release_link.id, image.height, image.width, image.type, 
        'http://s.dsimg.com/image/'||image.uri AS url
	FROM dblink(:dblink, '
		WITH unique_do_release_link AS (
			SELECT *
			FROM do_release_link
			WHERE gid IN (
				SELECT gid
				FROM do_release_link
				GROUP BY gid
				HAVING count(url) = 1
			)
			AND url IN (
				SELECT url
				FROM do_release_link
				GROUP BY url
				HAVING count(gid) = 1
			)
			AND edits_pending = 0
		)
		SELECT gid
		FROM unique_do_release_link
		LEFT JOIN release_meta ON release_meta.id = unique_do_release_link.id
		WHERE release_meta.amazon_asin ISNULL AND cover_art_presence = ''absent''::cover_art_presence
	') AS t1(gid uuid)
	JOIN mb_release_link ON mb_release_link.gid = t1.gid
	JOIN releases_images ON releases_images.release_id = mb_release_link.id
	JOIN image ON (image.id = releases_images.image_id AND (image.type = 'primary' OR (image.height >= 500 AND image.width >= 500)))
), score AS (
	SELECT gid, height*width AS score
	FROM images
	WHERE type = 'primary'
)
SELECT images.*
INTO TEMP img_tmp
FROM images
JOIN score ON score.gid = images.gid
ORDER BY score.score DESC, gid;

INSERT INTO update_log(updated, event) VALUES ('report_images', 'temp ready');

COPY img_tmp TO :filename (FORMAT CSV);

INSERT INTO update_log(updated, event) 
VALUES ('report_images', (SELECT 'Report contains: '
		||(SELECT count(*) FROM img_tmp)
		||' images for '
		||(SELECT count(*) FROM img_tmp GROUP BY gid)
		||' releases'
	));
    
SELECT event FROM update_log ORDER BY id LIMIT 1;