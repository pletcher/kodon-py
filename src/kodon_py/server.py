import os

from flask import Flask, abort, render_template
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from kodon_py.database import db, alembic, run_migrations, Element, Textpart


def create_app(test_config=None, sqlite_database=None):
    app = Flask(__name__, instance_relative_config=True)
    if sqlite_database is None:
        sqlite_database = (
            f"sqlite:///{os.path.join(app.instance_path, "kodon-db.sqlite")}"
        )
    app.config.from_mapping(
        SECRET_KEY=os.getenv("FLASK_APP_SECRET_KEY", "dev"),
        SQLALCHEMY_ENGINES={"default": sqlite_database},
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

    @app.route("/hello")
    def hello():
        return "Hello, World!"

    @app.route("/<urn>")
    def passage(urn=None):
        if ":" not in urn:
            abort(400)

        urn_parts = urn.rsplit(":", 1)
        document_urn = urn_parts[0]
        citation = urn_parts[1] if len(urn_parts) > 1 else ""

        citation_parts = citation.split(".") if citation else []

        if len(citation_parts) <= 1:
            urn_prefix = document_urn + ":"
        else:
            parent_citation = ".".join(citation_parts[:-1])
            urn_prefix = f"{document_urn}:{parent_citation}"

        textparts = db.session.execute(
            select(Textpart)
            .filter(Textpart.urn.startswith(urn_prefix))
            .order_by(Textpart.idx)
        ).scalars().all()

        if not textparts:
            abort(404)

        textpart_ids = [tp.id for tp in textparts]

        elements = db.session.execute(
            select(Element)
            .options(joinedload(Element.tokens))
            .filter(Element.textpart_id.in_(textpart_ids))
        ).unique().scalars().all()

        elements_by_textpart = {}
        for element in elements:
            elements_by_textpart.setdefault(element.textpart_id, []).append(element)

        text_containers = []
        for textpart in textparts:
            top_level_elements = sorted(
                [
                    e
                    for e in elements_by_textpart.get(textpart.id, [])
                    if e.parent_id is None
                ],
                key=lambda e: e.idx,
            )
            text_containers.append(
                {
                    "urn": textpart.urn,
                    "children": [
                        element_to_dict(elements_by_textpart, e)
                        for e in top_level_elements
                    ],
                }
            )

        return render_template(
            "components/ReadingEnvironment.html.jinja",
            current_passage_urn=urn,
            text_containers=text_containers,
        )

    return app


def element_to_dict(elements_by_textpart: dict, element: Element) -> dict:
    """Convert an Element to a dict structure for templates."""
    result = {
        "tagname": element.tagname,
        "urn": element.urn,
        "children": [],
    }

    if element.attributes:
        result.update(element.attributes)

    for token in sorted(element.tokens, key=lambda t: t.position):
        if (
            not result["children"]
            or result["children"][-1].get("tagname") != "text_run"
        ):
            result["children"].append({"tagname": "text_run", "tokens": []})
        result["children"][-1]["tokens"].append(
            {
                "text": token.text,
                "urn": token.urn,
                "whitespace": token.whitespace,
            }
        )

    children = [
        e
        for e in elements_by_textpart.get(element.textpart_id, [])
        if e.parent_id == element.id
    ]
    for child in sorted(children, key=lambda e: e.idx):
        result["children"].append(element_to_dict(elements_by_textpart, child))

    return result
