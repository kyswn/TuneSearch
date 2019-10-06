insert into tfidf
select
song_id,
token.token as token,
count::float * log (57650::float / df::float) as score
from token
inner join (select token, count(*) as df from token group by token) temp
on temp.token = token.token;
