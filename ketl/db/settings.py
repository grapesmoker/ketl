import os
import configparser

from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session


CONFIG_FILE = Path('ketl.ini')

if CONFIG_FILE.exists():
    parser = configparser.ConfigParser()
    parser.read(CONFIG_FILE)
    if 'ketl' in parser:
        DB_DSN = parser.get('ketl', 'DB_DSN', fallback=os.getenv('DB_DSN', 'sqlite:///ketl.db'))
    else:
        DB_DSN = os.getenv('DB_DSN', 'sqlite:///ketl.db')
else:
    DB_DSN = os.getenv('DB_DSN', 'sqlite:///ketl.db')


def get_engine(conn_string=DB_DSN):
    # extremely primitive memoization to get a single global engine object
    return get_session.__dict__.setdefault(f'_engine:{conn_string}', create_engine(conn_string))


def get_session() -> Session:
    # extremely primitive memoization to get a single global session object
    s_maker = get_session.__dict__.setdefault('_sessionmaker', sessionmaker(bind=get_engine()))
    return get_session.__dict__.setdefault('_session', s_maker())

