import pytest
from tempfile import NamedTemporaryFile, TemporaryDirectory
from sqlalchemy.orm.session import close_all_sessions

from ketl.db.models import Base, API

metadata = [Base.metadata]


@pytest.fixture(scope='session')
def engine():

    from ketl.db.settings import get_engine

    return get_engine()


@pytest.fixture(scope='session')
def session(engine):

    from ketl.db.settings import get_session

    return get_session()


@pytest.fixture(scope='function', autouse=True)
def tables(engine):
    for m in metadata:
        m.create_all(engine)
    yield
    close_all_sessions()
    for m in metadata:
        m.drop_all(engine)
