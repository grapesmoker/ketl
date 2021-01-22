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


@pytest.yield_fixture(scope='function', autouse=True)
def tables(engine):
    for m in metadata:
        m.create_all(engine)
    yield
    close_all_sessions()
    for m in metadata:
        m.drop_all(engine)


@pytest.yield_fixture(scope='session')
def temp_dir():

    with TemporaryDirectory(dir='./tests') as td:
        yield td
        # context should automatically cleanup the test dir


# @pytest.yield_fixture
# def cleanup(engine):
#
#     yield
#     for table in Base.metadata.sorted_tables:
#         with engine.connect() as con:
#             trans = con.begin()
#             con.execute(table.delete())
#             trans.commit()
