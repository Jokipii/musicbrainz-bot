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

ALTER TABLE release_mb_mapping
ADD CONSTRAINT release_mb_mapping_release_id_fkey FOREIGN KEY (release_id) REFERENCES release(id);
