CREATE TABLE TIPO (
    id TEXT PRIMARY KEY,
    txt TEXT NOT NULL,
    abbr TEXT UNIQUE
);

CREATE TABLE ETAPA (
    id TEXT PRIMARY KEY,
    txt TEXT NOT NULL
);

CREATE TABLE CENTRO (
    id INTEGER PRIMARY KEY,
    area TEXT,
    tipo TEXT,
    nombre TEXT,
    domicilio TEXT,
    municipio TEXT,
    distrito TEXT,
    cp INTEGER,
    telefono TEXT,
    -- fax TEXT,
    email TEXT,
    titularidad TEXT NOT NULL,
    CONSTRAINT fk_centro_tipo
        FOREIGN KEY (tipo)
        REFERENCES TIPO(abbr)
        ON DELETE CASCADE
);

CREATE TABLE QUERY (
    id TEXT PRIMARY KEY,
    txt TEXT
);

CREATE TABLE QUERY_CENTRO (
    centro INTEGER,
    query TEXT,
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
    centro INTEGER,
    etapa TEXT,
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