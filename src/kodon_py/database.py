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
    parent_id: Mapped[Optional[int]] = mapped_column(ForeignKey("elements.id"))
    tagname: Mapped[str]
    textpart: Mapped["Textpart"] = relationship(back_populates="elements")
    textpart_id: Mapped[int] = mapped_column(ForeignKey("textparts.id"))
    tokens: Mapped[List["Token"]] = relationship(back_populates="element")
    urn: Mapped[str]

    # Self-referential relationship for parent/children
    parent: Mapped[Optional["Element"]] = relationship(
        back_populates="children",
        remote_side="Element.id",
    )
    children: Mapped[List["Element"]] = relationship(
        back_populates="parent",
    )


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
    tokens: Mapped[List["Token"]] = relationship(back_populates="textpart")
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
