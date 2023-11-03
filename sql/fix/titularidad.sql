select distinct
    c.id, c.titularidad, q.query
from
    centro c left join query_centro q on
        c.id = q.centro and
        q.query like 'titularidad%=S'
where
    c.titularidad not in ('PUB', 'PRI', 'CON') or 
    (c.titularidad='PUB' and (q.query is NULL or q.query!='titularidadPublica=S')) or
    (c.titularidad='PRI' and (q.query is NULL or q.query!='titularidadPrivada=S')) or
    (c.titularidad='CON' and (q.query is NULL or q.query!='titularidadPrivadaConc=S'))
;