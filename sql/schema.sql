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
    centro INTEGER NOT NULL,
    etapa  TEXT NOT NULL,
    inferido INTEGER DEFAULT 0 NOT NULL,
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
    centro INTEGER NOT NULL,
    nombre TEXT NOT NULL,
    titularidad TEXT,
    tipo TEXT,
    -- plazas TEXT,
    inferido INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (centro, nombre, titularidad),
    CONSTRAINT fk_etapa_centro_centro
        FOREIGN KEY (centro)
        REFERENCES CENTRO(id)
        ON DELETE CASCADE
);

CREATE TABLE EDUCACION_DIFERENCIADA (
    centro INTEGER NOT NULL,
    etapa TEXT NOT NULL,
    PRIMARY KEY (centro, etapa),
    CONSTRAINT fk_educacion_diferenciada_centro
        FOREIGN KEY (centro)
        REFERENCES CENTRO(id)
        ON DELETE CASCADE
);

CREATE TABLE ESPECIAL_DIFICULTAD (
    centro INTEGER NOT NULL PRIMARY KEY,
    CONSTRAINT fk_especial_dificultad_centro
        FOREIGN KEY (centro)
        REFERENCES CENTRO(id)
        ON DELETE CASCADE
);

CREATE TABLE CONCURSO (
    id TEXT NOT NULL PRIMARY KEY,
    txt TEXT NOT NULL,
    url TEXT NOT NULL
);

CREATE TABLE CONCURSO_ANEXO (
    concurso TEXT NOT NULL,
    anexo INTEGER NOT NULL,
    txt TEXT NOT NULL,
    url TEXT NOT NULL,
    PRIMARY KEY (concurso, anexo),
    CONSTRAINT fk_concurso
        FOREIGN KEY (concurso)
        REFERENCES CONCURSO(id)
        ON DELETE CASCADE
);

CREATE TABLE CONCURSO_ANEXO_CENTRO (
    centro INTEGER NOT NULL,
    concurso TEXT NOT NULL,
    anexo INTEGER NOT NULL,
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
('PUB', 'Público'),
('CON', 'Privado Concertado'),
('PRI', 'Privado');

