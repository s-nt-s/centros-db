CREATE TABLE TIPO (
    id  TEXT NOT NULL PRIMARY KEY,
    txt TEXT NOT NULL,
    abr TEXT NOT NULL UNIQUE
);

CREATE TABLE ETAPA (
    id  TEXT NOT NULL PRIMARY KEY,
    txt TEXT NOT NULL
);

CREATE TABLE AREA (
    id  TEXT NOT NULL PRIMARY KEY,
    txt TEXT NOT NULL
);

CREATE TABLE TITULARIDAD (
    id  TEXT NOT NULL PRIMARY KEY,
    txt TEXT NOT NULL
);

CREATE TABLE JORNADA (
    id  TEXT NOT NULL PRIMARY KEY,
    txt TEXT NOT NULL
);

CREATE TABLE CENTRO (
    id          INTEGER NOT NULL PRIMARY KEY,
    area        TEXT REFERENCES AREA(id),
    tipo        TEXT NOT NULL,
    nombre      TEXT NOT NULL,
    titularidad TEXT NOT NULL REFERENCES TITULARIDAD(id),
    titular     TEXT,
    latitud     REAL,
    longitud    REAL,
    telefono    TEXT,
    email       TEXT,
    web         TEXT,
    domicilio   TEXT,
    municipio   TEXT NOT NULL,
    distrito    TEXT,
    cp          INTEGER,
    dificultad  INTEGER NOT NULL DEFAULT 0,
    jornada     TEXT NOT NULL REFERENCES JORNADA(id),
    -- fax TEXT,
    CONSTRAINT fk_centro_tipo
        FOREIGN KEY (tipo)
        REFERENCES TIPO(id)
        ON DELETE CASCADE
);

CREATE TABLE QUERY (
    id  TEXT NOT NULL PRIMARY KEY,
    txt TEXT
);

CREATE TABLE QUERY_CENTRO (
    centro INTEGER NOT NULL,
    query  TEXT NOT NULL,
    PRIMARY KEY (centro, query),
    CONSTRAINT fk_query_centro_centro
        FOREIGN KEY (centro)
        REFERENCES CENTRO(id)
        ON DELETE CASCADE,
    CONSTRAINT fk_query_centro_query
        FOREIGN KEY (query)
        REFERENCES QUERY(id)
        ON DELETE CASCADE
);

CREATE TABLE ETAPA_CENTRO (
    centro   INTEGER NOT NULL,
    etapa    TEXT NOT NULL,
    inferido INTEGER DEFAULT 0 NOT NULL,
    hoja INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (centro, etapa),
    CONSTRAINT fk_etapa_centro_centro
        FOREIGN KEY (centro)
        REFERENCES CENTRO(id)
        ON DELETE CASCADE,
    CONSTRAINT fk_etapa_centro_etapa
        FOREIGN KEY (etapa)
        REFERENCES ETAPA(id)
        ON DELETE CASCADE
);

CREATE TABLE ETAPA_NOMBRE_CENTRO (
    centro      INTEGER NOT NULL,
    nombre      TEXT NOT NULL,
    titularidad TEXT NOT NULL REFERENCES TITULARIDAD(id),
    tipo        TEXT,
    hoja        INTEGER DEFAULT 0 NOT NULL,
    -- plazas TEXT,
    PRIMARY KEY (centro, nombre, titularidad),
    CONSTRAINT fk_etapa_centro_centro
        FOREIGN KEY (centro)
        REFERENCES CENTRO(id)
        ON DELETE CASCADE
);

CREATE TABLE EDUCACION_DIFERENCIADA (
    centro INTEGER NOT NULL,
    tipo   TEXT NOT NULL,
    PRIMARY KEY (centro, tipo),
    CONSTRAINT fk_educacion_diferenciada_centro
        FOREIGN KEY (centro)
        REFERENCES CENTRO(id)
        ON DELETE CASCADE
);

CREATE TABLE EXTRAESCOLAR (
    centro INTEGER NOT NULL,
    nombre TEXT NOT NULL,
    PRIMARY KEY (centro, nombre),
    CONSTRAINT fk_extraescolar_centro
        FOREIGN KEY (centro)
        REFERENCES CENTRO(id)
        ON DELETE CASCADE
);

CREATE TABLE PLAN (
    centro INTEGER NOT NULL,
    nombre TEXT NOT NULL,
    PRIMARY KEY (centro, nombre),
    CONSTRAINT fk_plan_centro
        FOREIGN KEY (centro)
        REFERENCES CENTRO(id)
        ON DELETE CASCADE
);

CREATE TABLE PROYECTO (
    centro INTEGER NOT NULL,
    nombre TEXT NOT NULL,
    PRIMARY KEY (centro, nombre),
    CONSTRAINT fk_proyecto_centro
        FOREIGN KEY (centro)
        REFERENCES CENTRO(id)
        ON DELETE CASCADE
);

CREATE TABLE CONCURSO (
    convocatoria TEXT NOT NULL,
    tipo TEXT NOT NULL,
    id  TEXT NOT NULL PRIMARY KEY,
    txt TEXT NOT NULL,
    url TEXT NOT NULL,
    cuerpo TEXT NOT NULL
);

CREATE TABLE CONCURSO_ANEXO (
    concurso TEXT NOT NULL,
    anexo    INTEGER NOT NULL,
    txt      TEXT NOT NULL,
    url      TEXT NOT NULL,
    PRIMARY KEY (concurso, anexo),
    CONSTRAINT fk_concurso
        FOREIGN KEY (concurso)
        REFERENCES CONCURSO(id)
        ON DELETE CASCADE
);

CREATE TABLE CONCURSO_ANEXO_CENTRO (
    centro   INTEGER NOT NULL,
    concurso TEXT NOT NULL,
    anexo    INTEGER NOT NULL,
    PRIMARY KEY (centro, concurso, anexo),
    CONSTRAINT fk_concurso_centro
        FOREIGN KEY (centro)
        REFERENCES CENTRO(id)
        ON DELETE CASCADE,
    CONSTRAINT fk_concurso_anexo
        FOREIGN KEY (concurso, anexo)
        REFERENCES CONCURSO_ANEXO(concurso, anexo)
        ON DELETE CASCADE
);

INSERT INTO AREA VALUES
('E', 'Madrid-Este'),
('S', 'Madrid-Sur'),
('N', 'Madrid-Norte'),
('O', 'Madrid-Oeste'),
('C', 'Madrid-Capital');

INSERT INTO TITULARIDAD VALUES
('OTR', 'Otra'),
('PUB', 'Público'),
('CON', 'Concertado'),
('PRI', 'Privado');

INSERT INTO JORNADA VALUES
('C', 'Jornada continua'),
('O', 'Jornada partida');

CREATE VIEW CENTRO_TXT as
select * from (
    select centro, nombre txt from ETAPA_NOMBRE_CENTRO
    union
    select ec.centro, e.txt from ETAPA e join ETAPA_CENTRO ec on ec.etapa=e.id
    union
    select c.id centro, t.txt from CENTRO c join TIPO t on c.tipo=t.id
) order by centro
;