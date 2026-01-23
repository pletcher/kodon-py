"""
Two-phase ingestion pipeline for TEI XML documents.

Phase 1: Parse TEI XML files to JSON (intermediate storage)
Phase 2: Load JSON files into the SQLite database

Progress is tracked by file existence:
- Phase 1 complete: JSON file exists for the TEI source
- Phase 2 complete: Document URN exists in the database

Resumability:
- Parse phase skips files that already have JSON output
- Load phase skips documents that already exist in the database
"""

import json
import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Iterator

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = Path("./tei_json")


def discover_tei_files(source_dir: Path) -> Iterator[Path]:
    """
    Find all TEI XML files in the source directory.

    Args:
        source_dir: Root directory containing TEI XML files.

    Yields:
        Path objects for each .xml file found.
    """
    yield from source_dir.rglob("*.xml")


def get_json_path(tei_path: Path, source_dir: Path, output_dir: Path) -> Path:
    """
    Compute the JSON output path that mirrors the source directory structure.

    Args:
        tei_path: Path to the TEI XML file.
        source_dir: Root source directory.
        output_dir: Root output directory for JSON files.

    Returns:
        Path where the JSON file should be stored.
    """
    relative = tei_path.relative_to(source_dir)
    return output_dir / relative.with_suffix(".json")


def parse_tei_to_json(tei_path: Path, output_path: Path) -> dict:
    """
    Parse a TEI XML file and save the result as JSON.

    Args:
        tei_path: Path to the TEI XML file.
        output_path: Path where JSON output should be written.

    Returns:
        The parsed data dictionary.
    """
    from kodon_py.tei_parser import TEIParser

    logger.info(f"Parsing: {tei_path}")

    parser = TEIParser(tei_path)

    parsed_data = {
        "source_file": str(tei_path),
        "author": parser.author,
        "editionStmt": parser.editionStmt,
        "language": parser.language,
        "publicationStmt": parser.publicationStmt,
        "respStmt": parser.respStmt,
        "sourceDesc": parser.sourceDesc,
        "title": parser.title,
        "urn": parser.urn,
        "textpart_labels": parser.textpart_labels,
        "textparts": parser.textparts,
        "elements": parser.elements,
    }

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(parsed_data, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved JSON: {output_path}")

    return parsed_data


def json_to_parser_like(parsed_data: dict) -> SimpleNamespace:
    """
    Wrap parsed JSON data in an object that mimics TEIParser's interface.

    This allows reusing save_to_db which expects a TEIParser-like object.

    Args:
        parsed_data: Dict loaded from a parsed JSON file.

    Returns:
        SimpleNamespace with TEIParser-compatible attributes.
    """
    return SimpleNamespace(
        author=parsed_data.get("author"),
        editionStmt=parsed_data.get("editionStmt"),
        language=parsed_data.get("language"),
        publicationStmt=parsed_data.get("publicationStmt"),
        respStmt=parsed_data.get("respStmt"),
        sourceDesc=parsed_data.get("sourceDesc"),
        title=parsed_data.get("title"),
        urn=parsed_data.get("urn"),
        textpart_labels=parsed_data.get("textpart_labels", []),
        textparts=parsed_data.get("textparts", []),
        elements=parsed_data.get("elements", []),
    )


def document_exists(db_session, urn: str) -> bool:
    """
    Check if a document with the given URN exists in the database.

    Args:
        db_session: SQLAlchemy scoped session.
        urn: The document URN to check.

    Returns:
        True if document exists, False otherwise.
    """
    from kodon_py.database import Document

    return db_session.query(Document).filter(Document.urn == urn).first() is not None


def load_json_to_database(json_path: Path, db_session) -> str | None:
    """
    Load a parsed JSON file into the database.

    Skips documents that already exist in the database.

    Args:
        json_path: Path to the JSON file.
        db_session: SQLAlchemy scoped session.

    Returns:
        The URN of the loaded document, or None if skipped.
    """
    from kodon_py.api import save_to_db

    logger.info(f"Loading: {json_path}")

    with open(json_path, "r", encoding="utf-8") as f:
        parsed_data = json.load(f)

    urn = parsed_data.get("urn")

    if not urn:
        raise ValueError(f"No URN found in {json_path}")

    # Skip if already exists
    if document_exists(db_session, urn):
        logger.info(f"Skipping existing document: {urn}")
        return None

    # Wrap JSON data to look like TEIParser
    parser_like = json_to_parser_like(parsed_data)

    # Use existing save_to_db
    save_to_db(db_session, parser_like)

    logger.info(f"Loaded document: {urn}")

    return urn


def get_ingestion_status(
    source_dir: Path,
    output_dir: Path,
    db_session,
) -> dict:
    """
    Check the ingestion status of all TEI files.

    Args:
        source_dir: Root directory containing TEI XML files.
        output_dir: Root directory containing JSON output files.
        db_session: SQLAlchemy scoped session.

    Returns:
        Dict with status information:
        {
            "total": int,
            "parsed": int,
            "loaded": int,
            "files": [
                {
                    "tei_path": str,
                    "json_path": str,
                    "parsed": bool,
                    "loaded": bool,
                    "urn": str | None,
                },
                ...
            ]
        }
    """
    from kodon_py.database import Document

    files = []

    # Get all URNs currently in the database
    loaded_urns = set(
        urn for (urn,) in db_session.query(Document.urn).all()
    )

    for tei_path in discover_tei_files(source_dir):
        json_path = get_json_path(tei_path, source_dir, output_dir)
        parsed = json_path.exists()

        urn = None
        loaded = False

        if parsed:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                urn = data.get("urn")
                loaded = urn in loaded_urns

        files.append({
            "tei_path": str(tei_path),
            "json_path": str(json_path),
            "parsed": parsed,
            "loaded": loaded,
            "urn": urn,
        })

    return {
        "total": len(files),
        "parsed": sum(1 for f in files if f["parsed"]),
        "loaded": sum(1 for f in files if f["loaded"]),
        "files": files,
    }
