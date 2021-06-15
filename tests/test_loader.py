import pytest
import pandas as pd
import pickle

from hashlib import sha256
from tempfile import NamedTemporaryFile

from ketl.loader.Loader import (
    BaseLoader, DatabaseLoader, HashLoader, DelimitedFileLoader, ParquetLoader,
    LocalFileLoader, PickleLoader
)
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


def test_local_file_loader_csv(data_frame, tmp_path):

    csv_file = tmp_path / 'df.csv'
    with open(csv_file, 'w') as f:
        f.write('hello world')

    loader = DelimitedFileLoader(csv_file, index=False)

    assert loader.destination == csv_file
    assert loader.kwargs == {'index': False}
    assert not csv_file.exists()

    loader.load(data_frame)

    assert csv_file.exists()

    df_out = pd.read_csv(csv_file, index_col=None)

    assert df_out.equals(data_frame)


def test_local_file_loader_parquet(data_frame, tmp_path):

    parquet_file = tmp_path / 'df.parquet'

    loader = ParquetLoader(parquet_file, index=False)

    assert loader.destination == parquet_file
    assert loader.kwargs == {'index': False}
    assert not parquet_file.exists()

    loader.load(data_frame)
    loader.finalize()

    assert parquet_file.exists()

    df_out = pd.read_parquet(parquet_file)

    assert df_out.equals(data_frame)

    df1 = data_frame.copy(deep=True)
    df1.attrs['name'] = 'df1'
    df2 = data_frame.copy(deep=True)
    df2.attrs['name'] = 'df2'

    def naming_function(df):
        return df.attrs['name'] + '.parquet'

    pq_file1 = tmp_path / 'df1.parquet'
    pq_file2 = tmp_path / 'df2.parquet'

    loader = ParquetLoader(tmp_path, naming_func=naming_function, index=False)
    loader.load(df1)
    loader.load(df2)
    loader.finalize()

    df1_out = pd.read_parquet(pq_file1)
    df2_out = pd.read_parquet(pq_file2)

    assert data_frame.equals(df1_out)
    assert data_frame.equals(df2_out)


def test_local_file_loader_directory(tmp_path):

    with open(tmp_path / 'some_other_file.txt', 'w') as f:
        f.write('hello world')

    assert (tmp_path / 'some_other_file.txt').exists()

    _ = LocalFileLoader(tmp_path)

    assert not (tmp_path / 'some_other_file.txt').exists()


def test_local_file_loader_naming_func(data_frame, tmp_path):

    data_frame.attrs['name'] = 'my_nice_df'

    def naming_function(df):
        return df.attrs['name'] + '.csv'

    loader = LocalFileLoader(tmp_path, naming_func=naming_function)

    assert loader.full_path(data_frame) == tmp_path / 'my_nice_df.csv'


def test_local_file_loader_pickler(tmp_path):

    pickle_file = tmp_path / 'out.pickle'
    data = {'key': 'value'}

    loader = PickleLoader(pickle_file)
    loader.load(data)

    assert pickle_file.exists()
    result = pickle.load(open(pickle_file, 'rb'))
    assert result == data


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
