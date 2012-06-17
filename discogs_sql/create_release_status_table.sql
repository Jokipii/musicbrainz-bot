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
