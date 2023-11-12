select count(*) from (
	select 
		*
	from
		query_centro
	where
		query in ({0})
	group by
		centro
	having
		count(*)>1
)
;