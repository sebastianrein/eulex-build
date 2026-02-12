from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import Session

from .models import Base


def init_engine(db_url="sqlite:///eulex_build.db") -> Engine:
    engine = create_engine(db_url, echo=False)
    Base.metadata.create_all(engine)
    return engine


def get_session(engine: Engine) -> Session:
    return Session(engine)
