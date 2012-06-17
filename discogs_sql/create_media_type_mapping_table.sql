DROP TABLE IF EXISTS release_format;

SELECT DISTINCT 34 as format_id, '8cm CD' as format_name, release_id
INTO release_format
FROM releases_formats 
WHERE format_name = ANY(array['CD','CDr']) AND 'Mini' = ANY(descriptions)
	UNION
SELECT DISTINCT 25 as format_id, 'HDCD' as format_name, release_id
FROM releases_formats 
WHERE format_name = ANY(array['CD','CDr']) AND 'HDCD' = ANY(descriptions)
	UNION
SELECT DISTINCT 23 as format_id, 'SVCD' as format_name, release_id
FROM releases_formats 
WHERE format_name = ANY(array['CD','CDr']) AND 'SVCD' = ANY(descriptions)
	UNION
SELECT DISTINCT 22 as format_id, 'VCD' as format_name, release_id
FROM releases_formats 
WHERE format_name = ANY(array['CD','CDr', 'CDV']) AND 'VCD' = ANY(descriptions)
	UNION
SELECT DISTINCT 3 as format_id, 'SACD' as format_name, release_id
FROM releases_formats 
WHERE format_name = 'CD' AND 'SACD' = ANY(descriptions)
	UNION
SELECT DISTINCT 1 as format_id, 'CD' as format_name, release_id
FROM releases_formats
WHERE format_name = 'CD'
AND 'SACD' <> ALL(descriptions)
AND 'VCD' <> ALL(descriptions)
AND 'SVCD' <> ALL(descriptions)
AND 'HDCD' <> ALL(descriptions)
AND 'Mini' <> ALL(descriptions)
	UNION
SELECT DISTINCT 33 as format_id, 'CD-R' as format_name, release_id
FROM releases_formats 
WHERE format_name = 'CDr'
AND 'VCD' <> ALL(descriptions)
AND 'SVCD' <> ALL(descriptions)
AND 'HDCD' <> ALL(descriptions)
AND 'Mini' <> ALL(descriptions)
	UNION
SELECT DISTINCT 31 as format_id, '12" Vinyl' as format_name, release_id
FROM releases_formats 
WHERE format_name = ANY(array['Vinyl', 'Flexi-disc', 'Acetate', 'Shellac', 'Lathe Cut', 'Edison Disc'])
AND '12"' = ANY(descriptions)
	UNION
SELECT DISTINCT 30 as format_id, '10" Vinyl' as format_name, release_id
FROM releases_formats 
WHERE format_name = ANY(array['Vinyl', 'Flexi-disc', 'Acetate', 'Shellac', 'Lathe Cut', 'Edison Disc'])
AND '10"' = ANY(descriptions)
	UNION
SELECT DISTINCT 29 as format_id, '7" Vinyl' as format_name, release_id
FROM releases_formats 
WHERE format_name = ANY(array['Vinyl', 'Flexi-disc', 'Acetate', 'Shellac', 'Lathe Cut', 'Edison Disc'])
AND '7"' = ANY(descriptions)
	UNION
SELECT DISTINCT 7 as format_id, 'Vinyl' as format_name, release_id
FROM releases_formats 
WHERE format_name = ANY(array['Vinyl', 'Flexi-disc', 'Acetate', 'Shellac', 'Lathe Cut', 'Edison Disc'])
AND '12"' <> ALL(descriptions)
AND '10"' <> ALL(descriptions)
AND '7"' <> ALL(descriptions)
	UNION
SELECT DISTINCT 18 as format_id, 'DVD-Audio' as format_name, release_id
FROM releases_formats 
WHERE format_name = ANY(array['DVD','DVDr'])
AND 'DVD-Audio' = ANY(descriptions)
	UNION
SELECT DISTINCT 19 as format_id, 'DVD-Video' as format_name, release_id
FROM releases_formats 
WHERE format_name = ANY(array['DVD','DVDr'])
AND 'DVD-Video' = ANY(descriptions)
AND 'DVD-Audio' <> ALL(descriptions)
	UNION
SELECT DISTINCT 2 as format_id, 'DVD' as format_name, release_id
FROM releases_formats 
WHERE format_name = ANY(array['DVD','DVDr'])
AND 'DVD-Video' <> ALL(descriptions)
AND 'DVD-Audio' <> ALL(descriptions)
	UNION
SELECT DISTINCT 4 as format_id, 'DualDisc' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['DualDisc'])
	UNION
SELECT DISTINCT 5 as format_id, 'LaserDisc' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['Laserdisc'])
	UNION
SELECT DISTINCT 6 as format_id, 'MiniDisc' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['Minidisc'])
	UNION
SELECT DISTINCT 8 as format_id, 'Casette' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['Cassette','Microcassette'])
	UNION
SELECT DISTINCT 9 as format_id, 'Cartridge' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['4-Track Cartridge','8-Track Cartridge'])
	UNION
SELECT DISTINCT 10 as format_id, 'Reel-to-reel' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['Reel-To-Reel'])
	UNION
SELECT DISTINCT 11 as format_id, 'DAT' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['DAT'])
	UNION
SELECT DISTINCT 12 as format_id, 'Digital Media' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['File'])
	UNION
SELECT DISTINCT 13 as format_id, 'Other' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['Datassette','MVD','Hybrid','SelectaVision','Floppy Disk','Box Set','All Media'])
	UNION
SELECT DISTINCT 14 as format_id, 'Wax Cylinder' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['Cylinder','Pathé Disc'])
	UNION
SELECT DISTINCT 16 as format_id, 'DCC' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['DCC'])
	UNION
SELECT DISTINCT 17 as format_id, 'HD-DVD' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['HD DVD','HD DVD-R'])
	UNION
SELECT DISTINCT 20 as format_id, 'Blu-ray' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['Blu-ray','Blu-ray-R'])
	UNION
SELECT DISTINCT 21 as format_id, 'VHS' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['VHS'])
	UNION
SELECT DISTINCT 24 as format_id, 'Betamax' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['Betamax'])
	UNION
SELECT DISTINCT 26 as format_id, 'USB Flash Drive' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['Memory Stick'])
	UNION
SELECT DISTINCT 28 as format_id, 'UMD' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['UMD'])
	UNION
SELECT DISTINCT 32 as format_id, 'Videotape' as format_name, release_id
FROM releases_formats WHERE format_name = ANY(array['Video 2000'])
