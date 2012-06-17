SELECT DISTINCT artist.gid, 'Band/group/duo/duet/trio/quartet/orchestra/ensemble in disambiguation comment' AS note
FROM artist
WHERE type ISNULL AND lower(comment) LIKE ANY(array[
'%% band','%% band %%','band %%',
'%% group','%% group %%','group %%',
'%% trio','%% trio %%','trio %%',
'%% quartet','%% quartet %%','quartet %%',
'%% duo','%% duo %%','duo %%',
'%% duet','%% duet %%','duet %%',
'%% orchestra','%% orchestra %%','orchestra %%',
'%% ensemble','%% ensemble %%','ensemble %%'
])
AND NOT lower(comment) LIKE ANY(array[
'%%unknown%%','%%artist%%band%%','%%performed%%','%%feat%%','%%artist%%group%%','%%composer%%'
])
LIMIT %s
