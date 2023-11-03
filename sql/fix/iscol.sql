select
	count(*)
from
	centro c
where exists (
	select * from (
		select distinct
			centro
		from
			query_centro
		where
			query in ({0})
		group by
			centro
		having
			count(*)>1
	) q where q.centro=c.id
);