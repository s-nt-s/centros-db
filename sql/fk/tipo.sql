select distinct
    c.tipo
from
    CENTRO c
where
    not exists (select * from TIPO t where t.abbr=c.tipo)