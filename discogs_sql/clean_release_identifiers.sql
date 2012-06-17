UPDATE release_identifier
SET type = 'Label Code', description = Null
WHERE (type = 'Other' OR type = 'Barcode')
AND value ~* '^LC *[0-9]+$';

UPDATE release_identifier
SET type = 'Rights Society', description = Null
WHERE type = 'Other'
AND (description ~* 'Rights Society' OR description ~* 'Rights societies');

UPDATE release_identifier
SET type = 'Matrix / Runout'
WHERE type = 'Other'
AND description ~* 'runout';

UPDATE release_identifier SET value = replace(replace(replace(replace(replace(value, ' ', ''),'-',''),'>',''),'.',''),'/','')
WHERE type = 'Barcode'
AND replace(replace(replace(replace(replace(value, ' ', ''),'-',''),'>',''),'.',''),'/','') ~ '^[0-9]+$';

UPDATE release_identifier SET value = 'none'
WHERE type = 'Barcode'
AND (value ~* 'none' OR value ~* 'no barcode');

ALTER TABLE releases_labels ADD COLUMN label_id integer;
UPDATE releases_labels SET label_id = label.id FROM label WHERE label.name = releases_labels.label;