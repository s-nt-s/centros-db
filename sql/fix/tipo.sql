select distinct
    c.tipo
from
    CENTRO c
where
    not exists (select * from TIPO t where t.id=c.tipo)