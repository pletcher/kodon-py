"""Tests for the CLI module."""

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from kodon_py.cli import cli
from kodon_py.database import Model


@pytest.fixture
def cli_runner():
    """Create a Click CLI test runner."""
    return CliRunner()


@pytest.fixture
def initialized_db(temp_dir):
    """Create and initialize a test database."""
    from sqlalchemy import create_engine

    db_path = temp_dir / "test.sqlite"
    engine = create_engine(f"sqlite:///{db_path}")
    Model.metadata.create_all(engine)
    return db_path


class TestCliHelp:
    """Tests for CLI help commands."""

    def test_main_help(self, cli_runner):
        """Should show main help."""
        result = cli_runner.invoke(cli, ["--help"])

        assert result.exit_code == 0
        assert "Kodon TEI ingestion CLI" in result.output

    def test_ingest_help(self, cli_runner):
        """Should show ingest group help."""
        result = cli_runner.invoke(cli, ["ingest", "--help"])

        assert result.exit_code == 0
        assert "parse" in result.output
        assert "load" in result.output
        assert "all" in result.output
        assert "status" in result.output


class TestParseCommand:
    """Tests for the parse command."""

    def test_parse_creates_json(self, cli_runner, test_tei_dir, temp_dir):
        """Should create JSON files from TEI XML."""
        output_dir = temp_dir / "json_output"

        result = cli_runner.invoke(
            cli,
            ["ingest", "parse", str(test_tei_dir), "-o", str(output_dir)],
        )

        assert result.exit_code == 0
        assert "Parsed:" in result.output

        json_files = list(output_dir.rglob("*.json"))
        assert len(json_files) > 0

    def test_parse_skips_existing(self, cli_runner, test_tei_dir, temp_dir):
        """Should skip files that already have JSON output."""
        output_dir = temp_dir / "json_output"

        # First run
        cli_runner.invoke(
            cli,
            ["ingest", "parse", str(test_tei_dir), "-o", str(output_dir)],
        )

        # Second run - should skip
        result = cli_runner.invoke(
            cli,
            ["ingest", "parse", str(test_tei_dir), "-o", str(output_dir)],
        )

        assert result.exit_code == 0
        assert "Skipped: 1" in result.output or "Skipped:" in result.output

    def test_parse_no_files(self, cli_runner, temp_dir):
        """Should handle empty directories gracefully."""
        empty_dir = temp_dir / "empty"
        empty_dir.mkdir()

        result = cli_runner.invoke(
            cli,
            ["ingest", "parse", str(empty_dir)],
        )

        assert result.exit_code == 0
        assert "No TEI XML files found" in result.output


class TestLoadCommand:
    """Tests for the load command."""

    def test_load_requires_database(self, cli_runner, temp_dir):
        """Should error when database doesn't exist."""
        nonexistent_db = temp_dir / "nonexistent.sqlite"

        result = cli_runner.invoke(
            cli,
            ["ingest", "load", "-d", str(nonexistent_db)],
        )

        assert result.exit_code == 0  # Graceful exit
        assert "Database not found" in result.output

    def test_load_requires_json_files(self, cli_runner, initialized_db, temp_dir):
        """Should error when no JSON files exist."""
        empty_json_dir = temp_dir / "empty_json"
        empty_json_dir.mkdir()

        result = cli_runner.invoke(
            cli,
            ["ingest", "load", "-j", str(empty_json_dir), "-d", str(initialized_db)],
        )

        assert result.exit_code == 0  # Graceful exit
        assert "No JSON files found" in result.output

    def test_load_imports_json(self, cli_runner, initialized_db, sample_json_file):
        """Should load JSON files into database."""
        json_dir = sample_json_file.parent

        result = cli_runner.invoke(
            cli,
            ["ingest", "load", "-j", str(json_dir), "-d", str(initialized_db)],
        )

        assert result.exit_code == 0
        assert "Loaded: 1" in result.output

    def test_load_skips_existing(self, cli_runner, initialized_db, sample_json_file):
        """Should skip documents that already exist."""
        json_dir = sample_json_file.parent

        # First load
        cli_runner.invoke(
            cli,
            ["ingest", "load", "-j", str(json_dir), "-d", str(initialized_db)],
        )

        # Second load - should skip
        result = cli_runner.invoke(
            cli,
            ["ingest", "load", "-j", str(json_dir), "-d", str(initialized_db)],
        )

        assert result.exit_code == 0
        assert "Skipped: 1" in result.output


class TestAllCommand:
    """Tests for the all command."""

    def test_all_runs_both_phases(self, cli_runner, test_tei_dir, temp_dir):
        """Should run both parse and load phases."""
        from sqlalchemy import create_engine

        output_dir = temp_dir / "json_output"
        db_path = temp_dir / "test.sqlite"

        # Initialize database
        engine = create_engine(f"sqlite:///{db_path}")
        Model.metadata.create_all(engine)

        result = cli_runner.invoke(
            cli,
            [
                "ingest",
                "all",
                str(test_tei_dir),
                "-o",
                str(output_dir),
                "-d",
                str(db_path),
            ],
        )

        assert result.exit_code == 0
        assert "Phase 1" in result.output
        assert "Phase 2" in result.output


class TestStatusCommand:
    """Tests for the status command."""

    def test_status_shows_counts(self, cli_runner, test_tei_dir, initialized_db, temp_dir):
        """Should show file counts."""
        output_dir = temp_dir / "json_output"
        output_dir.mkdir()

        result = cli_runner.invoke(
            cli,
            [
                "ingest",
                "status",
                str(test_tei_dir),
                "-o",
                str(output_dir),
                "-d",
                str(initialized_db),
            ],
        )

        assert result.exit_code == 0
        assert "Total TEI files:" in result.output
        assert "Parsed to JSON:" in result.output
        assert "Loaded to database:" in result.output

    def test_status_verbose(self, cli_runner, test_tei_dir, initialized_db, temp_dir):
        """Should show file details in verbose mode."""
        output_dir = temp_dir / "json_output"
        output_dir.mkdir()

        result = cli_runner.invoke(
            cli,
            [
                "ingest",
                "status",
                str(test_tei_dir),
                "-o",
                str(output_dir),
                "-d",
                str(initialized_db),
                "-v",
            ],
        )

        assert result.exit_code == 0
        assert "File details:" in result.output
