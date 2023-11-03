select
    q.query
from
    QUERY_CENTRO q
where
    not exists (select * from CENTRO c where c.id=q.centro)
group by
    q.query
order by
    count(*) desc