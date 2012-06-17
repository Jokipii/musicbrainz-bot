SELECT l_artist_url.id, link_type
FROM l_artist_url
JOIN url ON url.id = l_artist_url.entity1
JOIN link ON link.id = l_artist_url.link
WHERE url.url LIKE ANY(array[
'http://rateyourmusic.com/%%',
'http://www.worldcat.org/%%',
'http://musicmoz.org/%%',
'http://www.45cat.com/%%',
'http://www.musik-sammler.de/%%',
'http://discografia.dds.it/%%',
'http://www.elnet.ee/ester/%%',
'http://www.encyclopedisque.fr/%%',
'http://www.discosdobrasil.com.br/%%',
'http://isrc.ncl.edu.tw/%%',
'http://www.rolldabeats.com/%%',
'http://www.psydb.net/%%',
'http://www.metal-archives.com/%%',
'http://www.spirit-of-metal.com/%%',
'http://www.ibdb.com/%%',
'http://www.iobdb.com/%%',
'http://theatricalia.com/%%',
'http://ocremix.org/%%'
])
AND l_artist_url.link = ANY(array[
26043,
26045,
26049,
26051,
26069
])
LIMIT %s
