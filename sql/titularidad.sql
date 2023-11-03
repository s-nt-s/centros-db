PRAGMA foreign_keys = ON;
delete from CENTRO where
    titularidad!='{tit}' and
    not exists (
        select * from QUERY_CENTRO q where
        q.query='{qr}' and
        q.centro=CENTRO.id
    )
;

delete from ETAPA where not exists (
    select * from ETAPA_CENTRO ec where ec.etapa=ETAPA.id
);

delete from TIPO where not exists (
    select * from CENTRO c where c.tipo=TIPO.abbr
);