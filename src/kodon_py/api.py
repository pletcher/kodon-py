from sqlalchemy.orm import Session, scoped_session

from kodon_py import models
from kodon_py.tei_parser import TEIParser


def save_to_db(db_session: scoped_session[Session], tei_parser: TEIParser):
    """Save parsed TEI data to SQLite database."""

    models.Base.query = db_session.query_property()

    if tei_parser.urn is None:
        raise ValueError("Cannot save a document without a URN.")

    try:
        # Save document metadata
        document = models.Document(
            editionStmt=tei_parser.editionStmt,
            language=tei_parser.language,
            publicationStmt=tei_parser.publicationStmt,
            respStmt=tei_parser.respStmt,
            sourceDesc=tei_parser.sourceDesc,
            title=tei_parser.title,
            urn=tei_parser.urn
        )
        db_session.add(document)

        # Save textparts and build a mapping of URNs to IDs
        textpart_id_map = {}
        for textpart_data in tei_parser.textparts:
            textpart = models.Textpart(
                idx=textpart_data["index"],
                location=textpart_data["location"],
                n=textpart_data["n"],
                subtype=textpart_data["subtype"],
                type=textpart_data["type"],
                urn=textpart_data["urn"]
            )

            document.textparts.append(textpart)
            db_session.flush()

            textpart_id_map[textpart.urn] = textpart.id

        # Save all top-level elements
        for element in tei_parser.elements:
            save_element(db_session, tei_parser.urn, textpart_id_map, element)

        db_session.commit()
    except Exception:
        db_session.rollback()
        raise


def save_element(
    db_session: scoped_session[Session],
    document_urn: str,
    textpart_id_map: dict,
    element: dict,
    parent_id: int | None = None
):
    """Recursively save an element and its children."""
    textpart_id = textpart_id_map.get(element.get("textpart_urn"))

    db_element = models.Element(
        attributes=element["attributes"],
        idx=element["index"],
        parent_id=parent_id,
        tagname=element["tagname"],
        textpart_id=textpart_id,
        urn=element["urn"],
    )

    db_session.add(db_element)
    db_session.flush()

    # Process children
    for child in element.get("children", []):
        if child.get("tagname") == "text_run":
            # Save tokens in this text run
            for position, token in enumerate(child.get("tokens", [])):
                db_token = models.Token(
                    position=position,
                    text=token["text"],
                    textpart_id=textpart_id,
                    urn=token["urn"],
                    whitespace=token["whitespace"],
                )

                db_element.tokens.append(db_token)
        else:
            # Recursively save child element
            save_element(db_session, document_urn, textpart_id_map, child, db_element.id)
