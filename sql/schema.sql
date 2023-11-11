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
    domicilio   TEXT,
    municipio   TEXT,
    distrito    TEXT,
    cp          INTEGER,
    telefono    TEXT,
    email       TEXT,
    titularidad TEXT NOT NULL REFERENCES TITULARIDAD(id),
    titular     TEXT,
    latitud     REAL,
    longitud    REAL,
    web         TEXT,
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
    CONSTRAINT fk_etapa_centro_centro
        FOREIGN KEY (centro)
        REFERENCES CENTRO(id)
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

