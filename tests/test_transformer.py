import pytest
import io
import pandas as pd
import json

from inflection import underscore
from pathlib import Path
from tempfile import NamedTemporaryFile

from ketl.transformer.Transformer import BaseTransformer, DelimitedTableTransformer, JsonTableTransformer


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

    s1.seek(0)
    s2.seek(0)

    for i, df in enumerate(transformer.transform([s1, s2])):
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

    s1.seek(0)
    s2.seek(0)

    for i, df in enumerate(transformer.transform([s1, s2])):
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


def test_json_transformer_init():

    kwargs = {'transpose': True, 'concat_on_axis': True, 'record_path': ['path1', 'path2'],
              'rename': underscore, 'columns': ['col1', 'col2']}
    transformer = JsonTableTransformer(**kwargs)

    assert transformer.transpose
    assert transformer.concat_on_axis
    assert transformer.record_path == ['path1', 'path2']
    assert transformer.columns == ['col1', 'col2']
    assert transformer.rename is not None


def test_json_transformer_extract_data(tmp_path):

    data = {
        'foo': {
            'bar': [
                {'field1': 'foo'},
                {'field2': 'bar'}
            ]
        }
    }

    tf = NamedTemporaryFile(dir=tmp_path, delete=False)
    tf.write(json.dumps(data).encode('utf-8'))
    tf.close()

    assert json.dumps(data['foo']['bar']) == JsonTableTransformer._extract_data(tf.name, ['foo', 'bar'])
    assert json.dumps(data['foo']) == JsonTableTransformer._extract_data(tf.name, 'foo')

    with pytest.raises(TypeError):
        JsonTableTransformer._extract_data(tf.name, 1)


def test_json_transformer_build_data_frame(tmp_path):

    df_expected = pd.DataFrame.from_records([('foo1', 'bar1'), ('foo2', 'bar2')], columns=['field1', 'field2'])

    data = [
        {'field1': 'foo1', 'field2': 'bar1'},
        {'field1': 'foo2', 'field2': 'bar2'}
    ]

    tf = NamedTemporaryFile(dir=tmp_path, delete=False)
    tf.write(json.dumps(data).encode('utf-8'))
    tf.close()

    transformer = JsonTableTransformer()

    for df in transformer.transform([tf.name]):
        assert df_expected.equals(df)

    data = {
        'foo': {
            'bar': [
                {'field1': 'foo1', 'field2': 'bar1'},
                {'field1': 'foo2', 'field2': 'bar2'}
            ]
        }
    }

    tf = NamedTemporaryFile(dir=tmp_path, delete=False)
    tf.write(json.dumps(data).encode('utf-8'))
    tf.close()

    transformer = JsonTableTransformer(record_path=['foo', 'bar'])

    for df in transformer.transform([tf.name]):
        assert df_expected.equals(df)

    # mock_record_path = mock.Mock()
    # mock_record_path.side_effect = [ValueError, ValueError]
    #
    # transformer.record_path = mock_record_path


def test_json_transformer_build_df_errors():

    transformer = JsonTableTransformer()

    transformer.skip_errors = True
    dfs = transformer._build_data_frame([Path('file1')])
    for df in dfs:
        assert df.empty


def test_json_transformer_snakecase_and_filter(tmp_path):

    df_expected = pd.DataFrame.from_records([('foo1', ), ('foo2', )], columns=['my_field1'])

    data = [
        {'myField1': 'foo1', 'myField2': 'bar1'},
        {'myField1': 'foo2', 'myField2': 'bar2'}
    ]

    tf = NamedTemporaryFile(dir=tmp_path, delete=False)
    tf.write(json.dumps(data).encode('utf-8'))
    tf.close()

    transformer = JsonTableTransformer(columns=['my_field1'], rename=underscore)

    for df in transformer.transform([tf.name]):
        assert df_expected.equals(df)
