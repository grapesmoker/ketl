import pytest
import zipfile
import gzip
import tarfile
import lzma
import shutil
import io
import pandas as pd

from tempfile import NamedTemporaryFile, TemporaryDirectory
from pathlib import Path
from hashlib import sha1

from tests.factories import APIFactory, SourceFactory, CachedFileFactory, ExpectedFileFactory, CredsFactory

from ketl.db.settings import get_session
from ketl.db import models
from ketl.transformer.Transformer import BaseTransformer, DelimitedTableTransformer


def test_base_transformer():

    transformer = BaseTransformer(**{'foo': 'bar'})

    assert transformer.passed_kwargs == {'foo': 'bar'}

    with pytest.raises(NotImplementedError):
        transformer.transform([Path('foo')])

    with pytest.raises(NotImplementedError):
        transformer._build_data_frame([Path('foo')])


def test_delimited_transformer_init():

    kwargs = {'foo': 'bar', 'transpose': True, 'concat_on_axis': True}
    transformer = DelimitedTableTransformer(**kwargs)

    assert transformer.transpose
    assert transformer.concat_on_axis
    assert transformer.reader_kwargs['foo'] == 'bar'


def test_build_data_frame():

    transformer = DelimitedTableTransformer(delimiter=',')

    data1 = 'x,y,z\n1,2,3\n4,5,6'
    data2 = 'x,y,z\n7,8,9\n10,11,12'
    s1 = io.StringIO()
    s1.write(data1)
    s1.seek(0)
    s2 = io.StringIO()
    s2.write(data2)
    s2.seek(0)

    df1_expected = pd.DataFrame.from_records([(1, 2, 3), (4, 5, 6)], columns=['x', 'y', 'z'])
    df2_expected = pd.DataFrame.from_records([(7, 8, 9), (10, 11, 12)], columns=['x', 'y', 'z'])

    for i, df in enumerate(transformer._build_data_frame([s1, s2])):
        assert len(df) == 2
        if i == 0:
            assert df.equals(df1_expected)
        elif i == 1:
            assert df.equals(df2_expected)
        else:
            assert False


def test_build_data_frame_transpose():

    transformer = DelimitedTableTransformer(delimiter=',', transpose=True)

    data1 = 'x,y,z\n1,2,3\n4,5,6'
    data2 = 'x,y,z\n7,8,9\n10,11,12'
    s1 = io.StringIO()
    s1.write(data1)
    s1.seek(0)
    s2 = io.StringIO()
    s2.write(data2)
    s2.seek(0)

    df1_expected = pd.DataFrame.from_records([(1, 4), (2, 5), (3, 6)], columns=[0, 1], index=['x', 'y', 'z'])
    df2_expected = pd.DataFrame.from_records([(7, 10), (8, 11), (9, 12)], columns=[0, 1], index=['x', 'y', 'z'])

    for i, df in enumerate(transformer._build_data_frame([s1, s2])):
        assert len(df) == 3
        if i == 0:
            assert df.equals(df1_expected)
        elif i == 1:
            assert df.equals(df2_expected)
        else:
            assert False


def test_build_data_frame_concat():

    transformer = DelimitedTableTransformer(delimiter=',', concat_on_axis=1, iterator=False, chunksize=None)

    data1 = 'x\n1\n2'
    data2 = 'y\n3\n4'
    s1 = io.StringIO()
    s1.write(data1)
    s1.seek(0)
    s2 = io.StringIO()
    s2.write(data2)
    s2.seek(0)

    df1_expected = pd.DataFrame.from_records([(1, 3), (2, 4)], columns=['x', 'y'])
    df2_expected = pd.DataFrame.from_records([(7, 10), (8, 11), (9, 12)], columns=[0, 1], index=['x', 'y', 'z'])

    for i, df in enumerate(transformer._build_data_frame([s1, s2])):
        assert len(df) == 2
        if i == 0:
            assert df.equals(df1_expected)
        else:
            assert False
