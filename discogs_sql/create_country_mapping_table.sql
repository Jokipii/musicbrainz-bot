DROP TABLE IF EXISTS country_mapping;
CREATE TABLE country_mapping
(
  id integer,
  iso_code character varying(2) NOT NULL,
  name text NOT NULL
);
INSERT INTO country_mapping VALUES (NULL,'AG','Antigua & Barbuda');
INSERT INTO country_mapping VALUES (NULL,'AN','Netherlands Antilles');
INSERT INTO country_mapping VALUES (NULL,'BA','Bosnia & Herzegovina');
INSERT INTO country_mapping VALUES (NULL,'BN','Brunei');
INSERT INTO country_mapping VALUES (NULL,'BO','Bolivia');
INSERT INTO country_mapping VALUES (NULL,'BS','Bahamas, The');
INSERT INTO country_mapping VALUES (NULL,'CD','Congo, Democratic Republic of the');
INSERT INTO country_mapping VALUES (NULL,'CG','Congo, Republic of the');
INSERT INTO country_mapping VALUES (NULL,'CS','Serbia and Montenegro');
INSERT INTO country_mapping VALUES (NULL,'GB','UK');
INSERT INTO country_mapping VALUES (NULL,'IR','Iran');
INSERT INTO country_mapping VALUES (NULL,'KP','North Korea');
INSERT INTO country_mapping VALUES (NULL,'KR','South Korea');
INSERT INTO country_mapping VALUES (NULL,'MD','Moldova');
INSERT INTO country_mapping VALUES (NULL,'MK','Macedonia');
INSERT INTO country_mapping VALUES (NULL,'MM','Burma');
INSERT INTO country_mapping VALUES (NULL,'MO','Macau');
INSERT INTO country_mapping VALUES (NULL,'PF','Clipperton Island');
INSERT INTO country_mapping VALUES (NULL,'PN','Pitcairn Islands');
INSERT INTO country_mapping VALUES (NULL,'RE','Reunion');
INSERT INTO country_mapping VALUES (NULL,'RU','Russia');
INSERT INTO country_mapping VALUES (NULL,'SU','USSR');
INSERT INTO country_mapping VALUES (NULL,'SY','Syria');
INSERT INTO country_mapping VALUES (NULL,'TF','French Southern & Antarctic Lands');
INSERT INTO country_mapping VALUES (NULL,'TF','Europa Island');
INSERT INTO country_mapping VALUES (NULL,'TL','East Timor');
INSERT INTO country_mapping VALUES (NULL,'TT','Trinidad & Tobago');
INSERT INTO country_mapping VALUES (NULL,'TZ','Tanzania');
INSERT INTO country_mapping VALUES (NULL,'UM','Wake Island');
INSERT INTO country_mapping VALUES (NULL,'UM','Kingman Reef');
INSERT INTO country_mapping VALUES (NULL,'US','US');
INSERT INTO country_mapping VALUES (NULL,'VA','Vatican City');
INSERT INTO country_mapping VALUES (NULL,'VA','Holy See (Vatican City)');
INSERT INTO country_mapping VALUES (NULL,'VC','Saint Vincent and the Grenadines');
INSERT INTO country_mapping VALUES (NULL,'VE','Venezuela');
INSERT INTO country_mapping VALUES (NULL,'VN','Vietnam');
INSERT INTO country_mapping VALUES (NULL,'XC','Czechoslovakia');
INSERT INTO country_mapping VALUES (NULL,'XG','German Democratic Republic (GDR)');
INSERT INTO country_mapping VALUES (NULL,'YU','Yugoslavia');
WITH country AS (
    SELECT id, iso_code, name 
    FROM dblink(:dblink, 'SELECT id, iso_code, name FROM country') 
    AS t1(id integer, iso_code character varying(2), name text)
)
UPDATE country_mapping SET id = (SELECT country.id FROM country WHERE country_mapping.iso_code = country.iso_code);
WITH country AS (
    SELECT id, iso_code, name 
    FROM dblink(:dblink, 'SELECT id, iso_code, name FROM country') 
    AS t1(id integer, iso_code character varying(2), name text)
)
INSERT INTO country_mapping SELECT * FROM country;
