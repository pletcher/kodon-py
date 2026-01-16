from typing import List, Optional

from sqlalchemy import JSON, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from kodon_py.database import Base


class Document(Base):
    id: Mapped[int] = mapped_column(primary_key=True)
    editionStmt: Mapped[str]
    elements: Mapped[List["Element"]] = relationship(back_populates="document")
    language: Mapped[str]
    publicationStmt: Mapped[str]
    respStmt: Mapped[str]
    sourceDesc: Mapped[str]
    textgroup: Mapped[str]
    textparts: Mapped[List["Textpart"]] = relationship(back_populates="document")
    tokens: Mapped[List["Token"]] = relationship(back_populates="document")
    title: Mapped[str]
    urn: Mapped[str] = mapped_column(unique=True)


class Element(Base):
    id: Mapped[int] = mapped_column(primary_key=True)
    attributes: Mapped[Optional[JSON]]
    document: Mapped["Document"] = relationship(back_populates="elements")
    document_urn: Mapped[str] = mapped_column(ForeignKey("documents.urn"))
    idx: Mapped[int]
    parent: Mapped[Optional["Element"]] = relationship(back_populates="elements")
    parent_id: Mapped[Optional[int]] = mapped_column(ForeignKey("elements.id"))
    tagname: Mapped[str]
    textpart: Mapped["Textpart"] = relationship(back_populates="elements")
    textpart_id: Mapped[int] = mapped_column(ForeignKey("textparts.id"))
    tokens: Mapped[List["Token"]] = relationship(back_populates="element")
    urn: Mapped[str]


class Textpart(Base):
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


class Token(Base):
    id: Mapped[int] = mapped_column(primary_key=True)
    document: Mapped["Document"] = relationship(back_populates="tokens")
    document_urn: Mapped[str] = mapped_column(ForeignKey("documents.urn"))
    element: Mapped["Element"] = relationship(back_populates="tokens")
    element_id: Mapped[int] = mapped_column(ForeignKey("elements.id"))
    position: Mapped[int]
    text: Mapped[str]
    textpart: Mapped["Textpart"] = relationship(back_populates="tokens")
    textpart_id: Mapped[int] = mapped_column(ForeignKey("textparts.id"))
    urn: Mapped[str]
    whitespace: Mapped[bool]
