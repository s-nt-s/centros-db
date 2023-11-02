CREATE TABLE CENTRO (
    id INTEGER PRIMARY KEY,
    area TEXT,
    tipo TEXT,
    nombre TEXT,
    domicilio TEXT,
    municipio TEXT,
    distrito TEXT,
    cp TEXT,
    telefono TEXT,
    fax TEXT,
    email1 TEXT,
    email2 TEXT,
    titularidad TEXT
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