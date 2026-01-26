"""
CLI for kodon-py TEI ingestion pipeline.

Usage:
    kodon ingest parse ./tei-sources
    kodon ingest load
    kodon ingest all ./tei-sources
    kodon ingest status ./tei-sources
"""

import logging
from pathlib import Path

import click
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from kodon_py.ingestion import (
    DEFAULT_OUTPUT_DIR,
    discover_tei_files,
    get_ingestion_status,
    get_json_path,
    load_json_to_database,
    parse_tei_to_json,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_db_session(db_path: Path, create_if_missing: bool = False):
    """Create a database session, optionally creating the database if it doesn't exist."""
    from flask import Flask
    from kodon_py.database import db, alembic

    db_exists = db_path.exists()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Create a minimal Flask app to use Alembic for database setup
    app = Flask(__name__)
    app.config["SQLALCHEMY_ENGINES"] = {"default": f"sqlite:///{db_path}"}
    db.init_app(app)
    alembic.init_app(app)

    if create_if_missing and not db_exists:
        with app.app_context():
            alembic.upgrade()
        logger.info(f"Created database: {db_path}")

    engine = create_engine(f"sqlite:///{db_path}")
    session_factory = sessionmaker(bind=engine)
    return scoped_session(session_factory)


@click.group()
def cli():
    """Kodon TEI ingestion CLI."""
    pass


@cli.group()
def ingest():
    """TEI XML ingestion commands."""
    pass


@ingest.command("parse")
@click.argument("source_dir", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--output-dir", "-o",
    type=click.Path(path_type=Path),
    default=DEFAULT_OUTPUT_DIR,
    help=f"Output directory for JSON files (default: {DEFAULT_OUTPUT_DIR})",
)
def parse_command(source_dir: Path, output_dir: Path):
    """Parse TEI XML files to JSON. Skips files that already have JSON output."""
    source_dir = source_dir.resolve()
    output_dir = output_dir.resolve()

    tei_files = [
        t for t in list(discover_tei_files(source_dir)) if "__cts__" not in t.name
    ]
    total = len(tei_files)

    if total == 0:
        click.echo("No TEI XML files found.")
        return

    click.echo(f"Found {total} TEI XML files in {source_dir}")

    parsed = 0
    skipped = 0
    errors = 0

    with click.progressbar(tei_files, label="Parsing") as files:
        for tei_path in files:
            json_path = get_json_path(tei_path, source_dir, output_dir)

            if json_path.exists():
                skipped += 1
                continue

            try:
                parse_tei_to_json(tei_path, json_path)
                parsed += 1
            except Exception as e:
                errors += 1
                logger.error(f"Failed to parse {tei_path}: {e}")

    click.echo(f"\nParsed: {parsed}, Skipped: {skipped}, Errors: {errors}")


@ingest.command("load")
@click.option(
    "--json-dir", "-j",
    type=click.Path(exists=True, path_type=Path),
    default=DEFAULT_OUTPUT_DIR,
    help=f"Directory containing JSON files (default: {DEFAULT_OUTPUT_DIR})",
)
@click.option(
    "--db-path", "-d",
    type=click.Path(path_type=Path),
    default=Path("kodon-db.sqlite"),
    help="Path to SQLite database (default: kodon-db.sqlite)",
)
def load_command(json_dir: Path, db_path: Path):
    """Load parsed JSON files into the database. Skips documents that already exist."""
    json_dir = json_dir.resolve()
    db_path = db_path.resolve()

    db_session = get_db_session(db_path, create_if_missing=True)

    json_files = list(json_dir.rglob("*.json"))
    total = len(json_files)

    if total == 0:
        click.echo("No JSON files found. Run 'kodon ingest parse' first.")
        return

    click.echo(f"Found {total} JSON files in {json_dir}")

    loaded = 0
    skipped = 0
    errors = 0

    with click.progressbar(json_files, label="Loading") as files:
        for json_path in files:
            try:
                result = load_json_to_database(json_path, db_session)
                if result is None:
                    skipped += 1
                else:
                    loaded += 1
            except Exception as e:
                errors += 1
                logger.error(f"Failed to load {json_path}: {e}")
                db_session.rollback()

    db_session.remove()
    click.echo(f"\nLoaded: {loaded}, Skipped: {skipped}, Errors: {errors}")


@ingest.command("all")
@click.argument("source_dir", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--output-dir", "-o",
    type=click.Path(path_type=Path),
    default=DEFAULT_OUTPUT_DIR,
    help=f"Output directory for JSON files (default: {DEFAULT_OUTPUT_DIR})",
)
@click.option(
    "--db-path", "-d",
    type=click.Path(path_type=Path),
    default=Path("kodon-db.sqlite"),
    help="Path to SQLite database (default: kodon-db.sqlite)",
)
@click.pass_context
def all_command(ctx, source_dir: Path, output_dir: Path, db_path: Path):
    """Parse TEI XML files and load them into the database. Resumable."""
    click.echo("Phase 1: Parsing TEI XML to JSON\n")
    ctx.invoke(parse_command, source_dir=source_dir, output_dir=output_dir)

    click.echo("\nPhase 2: Loading JSON to database\n")
    ctx.invoke(load_command, json_dir=output_dir, db_path=db_path)


@ingest.command("status")
@click.argument("source_dir", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--output-dir", "-o",
    type=click.Path(path_type=Path),
    default=DEFAULT_OUTPUT_DIR,
    help=f"Output directory for JSON files (default: {DEFAULT_OUTPUT_DIR})",
)
@click.option(
    "--db-path", "-d",
    type=click.Path(path_type=Path),
    default=Path("kodon-db.sqlite"),
    help="Path to SQLite database (default: kodon-db.sqlite)",
)
@click.option(
    "--verbose", "-v",
    is_flag=True,
    help="Show details for each file",
)
def status_command(source_dir: Path, output_dir: Path, db_path: Path, verbose: bool):
    """Check ingestion status of TEI files."""
    source_dir = source_dir.resolve()
    output_dir = output_dir.resolve()
    db_path = db_path.resolve()

    db_session = get_db_session(db_path, create_if_missing=True)
    status = get_ingestion_status(source_dir, output_dir, db_session)
    db_session.remove()

    click.echo(f"Total TEI files:    {status['total']}")
    click.echo(f"Parsed to JSON:     {status['parsed']}")
    click.echo(f"Loaded to database: {status['loaded']}")

    pending_parse = status["total"] - status["parsed"]
    pending_load = status["parsed"] - status["loaded"]

    if pending_parse > 0:
        click.echo(f"\nPending parse: {pending_parse} files")
    if pending_load > 0:
        click.echo(f"Pending load:  {pending_load} files")

    if verbose:
        click.echo("\nFile details:")
        for f in status["files"]:
            parsed_mark = "+" if f["parsed"] else "-"
            loaded_mark = "+" if f["loaded"] else "-"
            click.echo(f"  [{parsed_mark}P {loaded_mark}L] {f['tei_path']}")
            if f["urn"]:
                click.echo(f"           URN: {f['urn']}")


if __name__ == "__main__":
    cli()
