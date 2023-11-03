CREATE TABLE JORNADA (
    id  TEXT NOT NULL PRIMARY KEY,
    txt TEXT NOT NULL
);

INSERT INTO JORNADA
select
    substr(id, -1, 1) id,
    txt
from
    QUERY
where
    id in ('checkOrdinaria=O', 'checkContinua=C')
;

ALTER TABLE CENTRO ADD COLUMN jornada TEXT REFERENCES JORNADA(id);
UPDATE CENTRO SET jornada = (
    select substr(query, -1, 1)
    from QUERY_CENTRO
    where
        centro=CENTRO.id and
        query in ('checkOrdinaria=O', 'checkContinua=C')
)
;

PRAGMA foreign_keys = ON;
delete from QUERY where id in ('checkOrdinaria=O', 'checkContinua=C');