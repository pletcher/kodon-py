"""
Copyright 2025 Galenus Verbatim

SQLite database module for storing parsed TEI data.
"""

import json
import sqlite3
from pathlib import Path
from typing import Any


class TEIDatabase:
    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.create_tables()

    def create_tables(self):
        """Create the database schema for TEI data."""
        cursor = self.conn.cursor()

        # Documents table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                urn TEXT PRIMARY KEY,
                lang TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Textparts table
        cursor.execute("""
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
            )
        """)

        # Elements table
        cursor.execute("""
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
            )
        """)

        # Tokens table
        cursor.execute("""
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
            )
        """)

        # Create indices for common queries
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_textparts_document ON textparts(document_urn)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_elements_document ON elements(document_urn)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_elements_textpart ON elements(textpart_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_tokens_document ON tokens(document_urn)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_tokens_textpart ON tokens(textpart_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_tokens_element ON tokens(element_id)"
        )

        self.conn.commit()

    def insert_document(self, urn: str, lang: str):
        """Insert a document record."""
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO documents (urn, lang) VALUES (?, ?)", (urn, lang)
        )
        self.conn.commit()

    def insert_textpart(self, textpart: dict) -> int:
        """Insert a textpart and return its ID."""
        cursor = self.conn.cursor()

        location_str = ".".join(textpart.get("location", []))

        cursor.execute(
            """
            INSERT INTO textparts (document_urn, urn, type, subtype, n, idx, location)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                textpart.get("urn", "").split(":")[0] + ":"
                + textpart.get("urn", "").split(":")[1],  # Extract document URN
                textpart.get("urn"),
                textpart.get("type"),
                textpart.get("subtype"),
                textpart.get("n"),
                textpart.get("index"),
                location_str,
            ),
        )

        self.conn.commit()
        return cursor.lastrowid

    def insert_element(
        self, element: dict, textpart_id: int | None, parent_id: int | None = None
    ) -> int:
        """Insert an element and return its ID."""
        cursor = self.conn.cursor()

        # Extract document URN from element URN
        urn_parts = element.get("urn", "").split(":")
        if len(urn_parts) >= 2:
            document_urn = urn_parts[0] + ":" + urn_parts[1]
        else:
            document_urn = element.get("textpart_urn", "").split(":")[0:2]
            document_urn = ":".join(document_urn) if document_urn else ""

        # Store additional attributes as JSON, excluding known fields
        excluded_attrs = {
            "urn",
            "tagname",
            "index",
            "textpart_urn",
            "textpart_index",
            "children",
        }
        extra_attrs = {k: v for k, v in element.items() if k not in excluded_attrs}
        attributes_json = json.dumps(extra_attrs) if extra_attrs else None

        cursor.execute(
            """
            INSERT INTO elements
            (document_urn, textpart_id, urn, tagname, idx, textpart_urn, textpart_index, parent_id, attributes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                document_urn,
                textpart_id,
                element.get("urn"),
                element.get("tagname"),
                element.get("index"),
                element.get("textpart_urn"),
                element.get("textpart_index"),
                parent_id,
                attributes_json,
            ),
        )

        self.conn.commit()
        return cursor.lastrowid

    def insert_token(
        self,
        token: dict,
        document_urn: str,
        textpart_id: int | None,
        element_id: int | None,
        position: int,
    ) -> int:
        """Insert a token and return its ID."""
        cursor = self.conn.cursor()

        cursor.execute(
            """
            INSERT INTO tokens
            (document_urn, textpart_id, element_id, urn, text, whitespace, position)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                document_urn,
                textpart_id,
                element_id,
                token.get("urn"),
                token.get("text"),
                token.get("whitespace", False),
                position,
            ),
        )

        self.conn.commit()
        return cursor.lastrowid

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
