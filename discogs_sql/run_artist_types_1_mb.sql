SELECT DISTINCT artist.gid, 'Singer/actor/producer/composer/conductor/dj in disambiguation comment' AS note
FROM artist
WHERE type ISNULL AND lower(comment) LIKE ANY(array[
'%% singer', '%% singer %%','singer %%',
'%% actor', '%% actor %%','actor %%',
'%% producer','%% producer %%','producer %%',
'%% composer','%% composer %%','composer %%',
'%% conductor','%% conductor %%','conductor %%',
'%% dj','%% dj %%','dj %%'
])
AND NOT lower(comment) LIKE ANY(array[
'%%unknown%%','%%feat%%','%%crew%%'
])
LIMIT %s
