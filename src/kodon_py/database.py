from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker


def init_db(sqlite_file: str):
    engine = create_engine(sqlite_file)
    db_session = scoped_session(sessionmaker(autocommit=False,
                                            autoflush=False,
                                            bind=engine))

    return db_session
