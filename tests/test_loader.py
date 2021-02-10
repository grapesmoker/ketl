import pytest
import pandas as pd

from hashlib import sha256
from tempfile import NamedTemporaryFile

from ketl.loader.Loader import BaseLoader, DatabaseLoader, DataFrameLoader, HashLoader
from ketl.db.settings import get_engine


@pytest.fixture
def data_frame():

    return pd.DataFrame.from_records([(1, 2, 3), (4, 5, 6)], columns=['x', 'y', 'z'])


def test_base_loader(data_frame):

    loader = BaseLoader('foo')

    assert loader.destination == 'foo'

    with pytest.raises(NotImplementedError):
        loader.load(data_frame)

    with pytest.raises(NotImplementedError):
        loader.finalize()


def test_hash_loader(data_frame, tmp_path):

    tf = NamedTemporaryFile(dir=tmp_path, delete=False)
    tf.close()

    loader = HashLoader(tf.name)

    expected_hash = sha256(pd.util.hash_pandas_object(data_frame).values).hexdigest()

    loader.load(data_frame)

    with open(tf.name, 'r') as f:
        data = f.read().strip()
        assert expected_hash == data

    loader.finalize()


def test_data_frame_loader_csv(data_frame, tmp_path):

    csv_file = tmp_path / 'df.csv'
    with open(csv_file, 'w') as f:
        f.write('hello world')

    loader = DataFrameLoader(csv_file, index=False)

    assert loader.dest_path == csv_file
    assert loader.file_format == DataFrameLoader.FileFormat.CSV
    assert loader.kwargs == {'index': False}
    assert not   csv_file.exists()

    loader.load(data_frame)

    assert csv_file.exists()

    df_out = pd.read_csv(csv_file, index_col=None)

    assert df_out.equals(data_frame)


def test_data_frame_loader_parquet(data_frame, tmp_path):

    parquet_file = tmp_path / 'df.parquet'

    loader = DataFrameLoader(parquet_file, index=False)

    assert loader.dest_path == parquet_file
    assert loader.file_format == DataFrameLoader.FileFormat.PARQUET
    assert loader.kwargs == {'index': False}
    assert not parquet_file.exists()

    loader.load(data_frame)
    loader.finalize()

    assert parquet_file.exists()

    df_out = pd.read_parquet(parquet_file)

    assert df_out.equals(data_frame)


def test_data_frame_loader_unknown():

    with pytest.raises(ValueError):
        loader = DataFrameLoader('some-file.unknown')


def test_database_loader_init():

    loader = DatabaseLoader('test_table', index=False)

    assert loader.schema is None
    assert loader.kwargs == {'index': False}
    assert not loader.clean

    loader = DatabaseLoader('test_table', schema='test_schema')

    assert loader.schema == 'test_schema'
    assert loader.kwargs == {}
    assert not loader.clean


def test_database_loader(data_frame):

    engine = get_engine()
    with engine.connect() as conn:
        conn.execute('DROP TABLE IF EXISTS test_table')
        conn.execute('CREATE TABLE IF NOT EXISTS test_table (x INTEGER, y INTEGER, z INTEGER)')

    loader = DatabaseLoader('test_table', index=False)

    assert loader.engine == engine
    assert loader.schema is None
    assert loader.kwargs == {'index': False}

    loader.load(data_frame)

    df_out = pd.read_sql_table('test_table', engine.connect(), index_col=False)

    assert df_out.equals(data_frame)
