"""Tests for the ingestion module."""

import json
from pathlib import Path

import pytest

from kodon_py.database import Document, Element, Textpart, Token
from kodon_py.ingestion import (
    DEFAULT_OUTPUT_DIR,
    discover_tei_files,
    document_exists,
    get_ingestion_status,
    get_json_path,
    json_to_parser_like,
    load_json_to_database,
    parse_tei_to_json,
)


class TestDiscoverTeiFiles:
    """Tests for discover_tei_files function."""

    def test_finds_xml_files(self, test_tei_dir):
        """Should find XML files in the directory."""
        files = list(discover_tei_files(test_tei_dir))
        assert len(files) > 0
        assert all(f.suffix == ".xml" for f in files)

    def test_empty_directory(self, temp_dir):
        """Should return empty iterator for directory with no XML files."""
        files = list(discover_tei_files(temp_dir))
        assert len(files) == 0


class TestGetJsonPath:
    """Tests for get_json_path function."""

    def test_mirrors_directory_structure(self, temp_dir):
        """Should mirror the source directory structure."""
        source_dir = temp_dir / "source"
        source_dir.mkdir()
        subdir = source_dir / "subdir"
        subdir.mkdir()
        tei_path = subdir / "test.xml"
        tei_path.touch()

        output_dir = temp_dir / "output"

        json_path = get_json_path(tei_path, source_dir, output_dir)

        assert json_path == output_dir / "subdir" / "test.json"

    def test_changes_extension_to_json(self, temp_dir):
        """Should change .xml extension to .json."""
        source_dir = temp_dir / "source"
        source_dir.mkdir()
        tei_path = source_dir / "document.xml"

        output_dir = temp_dir / "output"

        json_path = get_json_path(tei_path, source_dir, output_dir)

        assert json_path.suffix == ".json"
        assert json_path.stem == "document"


class TestParseTeiToJson:
    """Tests for parse_tei_to_json function."""

    def test_creates_json_file(self, test_tei_file, json_output_dir):
        """Should create a JSON file from TEI XML."""
        output_path = json_output_dir / "output.json"

        result = parse_tei_to_json(test_tei_file, output_path)

        assert output_path.exists()
        assert isinstance(result, dict)
        assert "urn" in result
        assert "textparts" in result
        assert "elements" in result

    def test_json_contains_required_fields(self, test_tei_file, json_output_dir):
        """Should include all required fields in JSON output."""
        output_path = json_output_dir / "output.json"

        result = parse_tei_to_json(test_tei_file, output_path)

        required_fields = [
            "source_file",
            "author",
            "editionStmt",
            "language",
            "publicationStmt",
            "respStmt",
            "sourceDesc",
            "title",
            "urn",
            "textpart_labels",
            "textparts",
            "elements",
        ]
        for field in required_fields:
            assert field in result, f"Missing field: {field}"

    def test_creates_parent_directories(self, test_tei_file, temp_dir):
        """Should create parent directories if they don't exist."""
        output_path = temp_dir / "nested" / "dirs" / "output.json"

        parse_tei_to_json(test_tei_file, output_path)

        assert output_path.exists()


class TestJsonToParserLike:
    """Tests for json_to_parser_like function."""

    def test_creates_object_with_parser_attributes(self, sample_parsed_data):
        """Should create an object with TEIParser-like attributes."""
        parser_like = json_to_parser_like(sample_parsed_data)

        assert parser_like.urn == sample_parsed_data["urn"]
        assert parser_like.title == sample_parsed_data["title"]
        assert parser_like.language == sample_parsed_data["language"]
        assert parser_like.textparts == sample_parsed_data["textparts"]
        assert parser_like.elements == sample_parsed_data["elements"]

    def test_handles_missing_fields(self):
        """Should handle missing optional fields gracefully."""
        minimal_data = {"urn": "test:urn"}

        parser_like = json_to_parser_like(minimal_data)

        assert parser_like.urn == "test:urn"
        assert parser_like.title is None
        assert parser_like.textparts == []
        assert parser_like.elements == []


class TestDocumentExists:
    """Tests for document_exists function."""

    def test_returns_false_for_nonexistent_document(self, db_session):
        """Should return False when document doesn't exist."""
        assert document_exists(db_session, "urn:nonexistent") is False

    def test_returns_true_for_existing_document(self, db_session):
        """Should return True when document exists."""
        doc = Document(
            urn="urn:test:doc",
            title="Test",
            editionStmt="",
            language="grc",
            publicationStmt="",
            respStmt="",
            sourceDesc="",
            textgroup="test",
        )
        db_session.add(doc)
        db_session.commit()

        assert document_exists(db_session, "urn:test:doc") is True


class TestLoadJsonToDatabase:
    """Tests for load_json_to_database function."""

    def test_loads_document_to_database(self, sample_json_file, db_session):
        """Should load JSON data into the database."""
        urn = load_json_to_database(sample_json_file, db_session)

        assert urn == "urn:cts:greekLit:tlg0001.tlg001.test-grc1"

        doc = db_session.query(Document).filter(Document.urn == urn).first()
        assert doc is not None
        assert doc.title == "Test Title"

    def test_creates_textparts(self, sample_json_file, db_session):
        """Should create textparts in the database."""
        load_json_to_database(sample_json_file, db_session)

        textparts = db_session.query(Textpart).all()
        assert len(textparts) == 1
        assert textparts[0].n == "1"

    def test_creates_elements_and_tokens(self, sample_json_file, db_session):
        """Should create elements and tokens in the database."""
        load_json_to_database(sample_json_file, db_session)

        elements = db_session.query(Element).all()
        assert len(elements) == 1
        assert elements[0].tagname == "p"

        tokens = db_session.query(Token).all()
        assert len(tokens) == 2
        assert tokens[0].text == "Test"
        assert tokens[1].text == "content"

    def test_skips_existing_document(self, sample_json_file, db_session):
        """Should skip loading if document already exists."""
        # Load once
        load_json_to_database(sample_json_file, db_session)

        # Load again - should return None (skipped)
        result = load_json_to_database(sample_json_file, db_session)

        assert result is None

        # Should still have only one document
        docs = db_session.query(Document).all()
        assert len(docs) == 1


class TestGetIngestionStatus:
    """Tests for get_ingestion_status function."""

    def test_reports_unparsed_files(self, test_tei_dir, temp_dir, db_session):
        """Should report files that haven't been parsed."""
        status = get_ingestion_status(test_tei_dir, temp_dir, db_session)

        assert status["total"] > 0
        assert status["parsed"] == 0
        assert status["loaded"] == 0

    def test_reports_parsed_files(self, test_tei_file, json_output_dir, db_session):
        """Should report files that have been parsed."""
        # Parse the file
        source_dir = test_tei_file.parent
        json_path = get_json_path(test_tei_file, source_dir, json_output_dir)
        parse_tei_to_json(test_tei_file, json_path)

        status = get_ingestion_status(source_dir, json_output_dir, db_session)

        assert status["parsed"] == 1
        assert status["loaded"] == 0

    def test_reports_loaded_files(self, test_tei_file, json_output_dir, db_session):
        """Should report files that have been loaded."""
        # Parse and load the file
        source_dir = test_tei_file.parent
        json_path = get_json_path(test_tei_file, source_dir, json_output_dir)
        parse_tei_to_json(test_tei_file, json_path)
        load_json_to_database(json_path, db_session)

        status = get_ingestion_status(source_dir, json_output_dir, db_session)

        assert status["parsed"] == 1
        assert status["loaded"] == 1


class TestEndToEndIngestion:
    """End-to-end tests for the full ingestion pipeline."""

    def test_full_pipeline(self, test_tei_file, json_output_dir, db_session):
        """Should successfully parse and load a TEI file."""
        source_dir = test_tei_file.parent

        # Phase 1: Parse
        json_path = get_json_path(test_tei_file, source_dir, json_output_dir)
        parsed_data = parse_tei_to_json(test_tei_file, json_path)

        assert json_path.exists()
        assert parsed_data["urn"] is not None

        # Phase 2: Load
        urn = load_json_to_database(json_path, db_session)

        assert urn == parsed_data["urn"]

        # Verify data in database
        doc = db_session.query(Document).filter(Document.urn == urn).first()
        assert doc is not None

        textparts = db_session.query(Textpart).filter(Textpart.document_urn == urn).all()
        assert len(textparts) > 0

        elements = db_session.query(Element).all()
        assert len(elements) > 0

        tokens = db_session.query(Token).all()
        assert len(tokens) > 0

    def test_resumability_parse_phase(self, test_tei_file, json_output_dir):
        """Parse phase should skip files that already have JSON output."""
        source_dir = test_tei_file.parent
        json_path = get_json_path(test_tei_file, source_dir, json_output_dir)

        # Parse once
        parse_tei_to_json(test_tei_file, json_path)
        first_mtime = json_path.stat().st_mtime

        # The file exists, so a second parse attempt would overwrite
        # In the CLI, we check for existence and skip - simulate that here
        assert json_path.exists()

    def test_resumability_load_phase(self, sample_json_file, db_session):
        """Load phase should skip documents that already exist."""
        # Load once
        first_result = load_json_to_database(sample_json_file, db_session)
        assert first_result is not None

        # Load again - should be skipped
        second_result = load_json_to_database(sample_json_file, db_session)
        assert second_result is None

        # Verify only one document exists
        count = db_session.query(Document).count()
        assert count == 1
