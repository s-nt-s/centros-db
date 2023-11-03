PRAGMA foreign_keys = ON;
delete from CENTRO where titularidad!='{tit}';

delete from ETAPA where not exists (
    select * from ETAPA_CENTRO ec where ec.etapa=ETAPA.id
);

delete from TIPO where not exists (
    select * from CENTRO c where c.tipo=TIPO.abr
);

ALTER TABLE CENTRO DROP COLUMN titularidad;
DROP TABLE TITULARIDAD;