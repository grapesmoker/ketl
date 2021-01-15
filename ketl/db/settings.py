import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session


DB_CONN_STRING = os.getenv('DB_DSN', 'sqlite:///ketl.db')


def get_engine():
    # extremely primitive memoization to get a single global engine object
    conn_string = os.getenv('DB_DSN', DB_CONN_STRING)
    return get_session.__dict__.setdefault(f'_engine:{conn_string}', create_engine(conn_string))


def get_session() -> Session:
    # extremely primitive memoization to get a single global session object
    Session = get_session.__dict__.setdefault('_sessionmaker', sessionmaker(bind=get_engine()))
    return get_session.__dict__.setdefault('_session', Session())
