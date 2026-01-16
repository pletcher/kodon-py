from kodon_py import database, models
from kodon_py.tei_parser import TEIParser


def save_to_db(tei_parser: TEIParser):
    """Save parsed TEI data to SQLite database."""

    if tei_parser.urn is None:
        raise ValueError("Cannot save a document without a URN.")

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
    database.db_session.add(document)
    database.db_session.commit()

    # Save textparts and build a mapping of URNs to IDs
    textpart_id_map = {}
    for textpart in tei_parser.textparts:
        textpart = models.Textpart(
            idx=textpart["index"],
            location=textpart["location"],
            n=textpart["n"],
            subtype=textpart["subtype"],
            type=textpart["type"],
            urn=textpart["urn"]
        )

        document.textparts.append(textpart)

        database.db_session.commit()

        textpart_id_map[textpart.get("urn")] = textpart.id

        # # Save tokens associated with this textpart
        # if "tokens" in textpart:
        #     for position, token in enumerate(textpart["tokens"]):
        #         token = models.Token(

        #         )
        #         insert_token(db,
        #             token,
        #             tei_parser.urn,
        #             textpart_id,
        #             None,
        #             position,
        #         )

    # Save all top-level elements
    for element in tei_parser.elements:
        save_element(tei_parser.urn, textpart_id_map, element)


def save_element(
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

    database.db_session.add(db_element)
    database.db_session.commit()

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

                database.db_session.commit()
        else:
            # Recursively save child element
            save_element(document_urn, textpart_id_map, child, db_element.id)
