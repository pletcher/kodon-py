import os

from flask import Flask, render_template
from flask_alembic import Alembic
from flask_sqlalchemy_lite import SQLAlchemy

from typing import Any, List, Optional

from sqlalchemy import ForeignKey
from sqlalchemy.types import JSON
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
)


class Model(DeclarativeBase):
    type_annotation_map = {dict[str, Any]: JSON}


db = SQLAlchemy()

alembic = Alembic(metadatas=Model.metadata)


class Document(Model):
    __tablename__ = "documents"

    id: Mapped[int] = mapped_column(primary_key=True)
    editionStmt: Mapped[str]
    language: Mapped[str]
    publicationStmt: Mapped[str]
    respStmt: Mapped[str]
    sourceDesc: Mapped[str]
    textgroup: Mapped[str]
    textparts: Mapped[List["Textpart"]] = relationship(back_populates="document")
    title: Mapped[str]
    urn: Mapped[str] = mapped_column(unique=True)


class Element(Model):
    __tablename__ = "elements"

    id: Mapped[int] = mapped_column(primary_key=True)
    attributes: Mapped[Optional[dict[str, Any]]]
    idx: Mapped[int]
    parent: Mapped[Optional["Element"]] = relationship(back_populates="elements")
    parent_id: Mapped[Optional[int]] = mapped_column(ForeignKey("elements.id"))
    tagname: Mapped[str]
    textpart: Mapped["Textpart"] = relationship(back_populates="elements")
    textpart_id: Mapped[int] = mapped_column(ForeignKey("textparts.id"))
    tokens: Mapped[List["Token"]] = relationship(back_populates="element")
    urn: Mapped[str]


class Textpart(Model):
    __tablename__ = "textparts"

    id: Mapped[int] = mapped_column(primary_key=True)
    document: Mapped["Document"] = relationship(back_populates="textparts")
    document_urn: Mapped[str] = mapped_column(ForeignKey("documents.urn"))
    elements: Mapped[List["Element"]] = relationship(back_populates="textpart")
    idx: Mapped[int]
    location: Mapped[Optional[str]]
    n: Mapped[Optional[str]]
    subtype: Mapped[Optional[str]]
    type: Mapped[Optional[str]]
    urn: Mapped[str] = mapped_column(unique=True)


class Token(Model):
    __tablename__ = "tokens"

    id: Mapped[int] = mapped_column(primary_key=True)
    element: Mapped["Element"] = relationship(back_populates="tokens")
    element_id: Mapped[int] = mapped_column(ForeignKey("elements.id"))
    position: Mapped[int]
    text: Mapped[str]
    textpart: Mapped["Textpart"] = relationship(back_populates="tokens")
    textpart_id: Mapped[int] = mapped_column(ForeignKey("textparts.id"))
    urn: Mapped[str]
    whitespace: Mapped[bool]


def run_migrations(app):
    """Run database migrations to head using alembic commands API."""
    with app.app_context():
        alembic.upgrade()


def create_app(test_config=None):
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY=os.getenv("FLASK_APP_SECRET_KEY", "dev"),
        SQLALCHEMY_ENGINES={
            "default": f"sqlite:///{os.path.join(app.instance_path, "kodon-db.sqlite")}"
        },
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile("config.py", silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    db.init_app(app)
    alembic.init_app(app)
    run_migrations(app)

    # a simple page that says hello
    @app.route("/hello")
    def hello():
        return "Hello, World!"

    @app.route("/<urn>")
    def passage(urn=None):
        return render_template("ReadingEnvironment.html.jinja", urn=urn)

    return app
