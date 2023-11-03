CREATE TABLE TIPO (
    cod TEXT PRIMARY KEY,
    txt TEXT NOT NULL,
    abbr TEXT UNIQUE
);

CREATE TABLE ETAPA (
    cod TEXT PRIMARY KEY,
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
    fax TEXT,
    email TEXT,
    titularidad TEXT NOT NULL,
    FOREIGN KEY (tipo) REFERENCES TIPO(abbr)
);

CREATE TABLE QUERY (
    id TEXT PRIMARY KEY,
    txt TEXT
);

CREATE TABLE QUERY_CENTRO (
    centro INTEGER,
    query TEXT,
    FOREIGN KEY (query)  REFERENCES QUERY(id),
    FOREIGN KEY (centro) REFERENCES CENTRO(id)
);