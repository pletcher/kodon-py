DROP TABLE IF EXISTS documents;
DROP TABLE IF EXISTS textparts;
DROP TABLE IF EXISTS elements;
DROP TABLE IF EXISTS tokens;


CREATE TABLE IF NOT EXISTS documents (
    urn TEXT PRIMARY KEY,
    lang TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS textparts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_urn TEXT NOT NULL,
    urn TEXT NOT NULL UNIQUE,
    type TEXT,
    subtype TEXT,
    n TEXT,
    idx INTEGER,
    location TEXT,
    FOREIGN KEY (document_urn) REFERENCES documents(urn)
);

CREATE TABLE IF NOT EXISTS elements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_urn TEXT NOT NULL,
    textpart_id INTEGER,
    urn TEXT NOT NULL,
    tagname TEXT NOT NULL,
    idx INTEGER,
    textpart_urn TEXT,
    textpart_index INTEGER,
    parent_id INTEGER,
    attributes TEXT,
    FOREIGN KEY (document_urn) REFERENCES documents(urn),
    FOREIGN KEY (textpart_id) REFERENCES textparts(id),
    FOREIGN KEY (parent_id) REFERENCES elements(id)
);

CREATE TABLE IF NOT EXISTS tokens (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_urn TEXT NOT NULL,
    textpart_id INTEGER,
    element_id INTEGER,
    urn TEXT NOT NULL,
    text TEXT NOT NULL,
    whitespace BOOLEAN,
    position INTEGER,
    FOREIGN KEY (document_urn) REFERENCES documents(urn),
    FOREIGN KEY (textpart_id) REFERENCES textparts(id),
    FOREIGN KEY (element_id) REFERENCES elements(id)
);

CREATE INDEX IF NOT EXISTS idx_textparts_document ON textparts(document_urn);
CREATE INDEX IF NOT EXISTS idx_elements_document ON elements(document_urn);
CREATE INDEX IF NOT EXISTS idx_elements_textpart ON elements(textpart_id);
CREATE INDEX IF NOT EXISTS idx_tokens_document ON tokens(document_urn);
CREATE INDEX IF NOT EXISTS idx_tokens_textpart ON tokens(textpart_id);
CREATE INDEX IF NOT EXISTS idx_tokens_element ON tokens(element_id);
