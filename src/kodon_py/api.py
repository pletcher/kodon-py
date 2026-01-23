from sqlalchemy.orm import Session, scoped_session

from kodon_py.database import Document, Element, Textpart, Token
from kodon_py.tei_parser import TEIParser


def save_to_db(db_session: scoped_session[Session], tei_parser: TEIParser):
    """Save parsed TEI data to SQLite database."""
    if tei_parser.urn is None:
        raise ValueError("Cannot save a document without a URN.")

    try:
        # Save document metadata
        document = Document(
            editionStmt=tei_parser.editionStmt,
            language=tei_parser.language,
            publicationStmt=tei_parser.publicationStmt,
            respStmt=tei_parser.respStmt,
            sourceDesc=tei_parser.sourceDesc,
            textgroup=tei_parser.author,
            title=tei_parser.title,
            urn=tei_parser.urn,
        )
        db_session.add(document)

        # Save textparts and build a mapping of URNs to IDs
        textpart_id_map = {}
        for textpart_data in tei_parser.textparts:
            location = textpart_data.get("location", [])
            if isinstance(location, list):
                location = ".".join(location)

            textpart = Textpart(
                idx=textpart_data["index"],
                location=location,
                n=textpart_data.get("n"),
                subtype=textpart_data.get("subtype"),
                type=textpart_data.get("type"),
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

    # Extract attributes (exclude internal keys used by the parser)
    internal_keys = {
        "children", "index", "tagname", "textpart_index",
        "textpart_urn", "urn"
    }
    attributes = {k: v for k, v in element.items() if k not in internal_keys}

    db_element = Element(
        attributes=attributes if attributes else None,
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
                db_token = Token(
                    position=position,
                    text=token["text"],
                    textpart_id=textpart_id,
                    urn=token["urn"],
                    whitespace=token.get("whitespace", False),
                )

                db_element.tokens.append(db_token)
        else:
            # Recursively save child element
            save_element(db_session, document_urn, textpart_id_map, child, db_element.id)
