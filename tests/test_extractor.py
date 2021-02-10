import pytest
import io
import pandas as pd
import random

from datetime import timedelta
from tqdm import tqdm
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest import mock

from ketl.db import models
from ketl.extractor.Extractor import SourceTargetPair, BaseExtractor, DefaultExtractor
from ketl.utils.file_utils import file_hash
from tests.factories import APIFactory, CredsFactory, SourceFactory, CachedFileFactory, ExpectedFileFactory


def test_base_extractor():

    extractor = BaseExtractor()
    with pytest.raises(NotImplementedError):

        extractor.extract()


def test_default_extractor_init():

    api: models.API = APIFactory(name='my nice api')
    creds_details = {
        'cookie': {
            'name': 'my-cookie',
            'value': 'my-value'
        },
        'auth': {
            'username': 'my-username',
            'password': 'my-password'
        },
        'auth_token': {
            'header': 'Token',
            'token': 'my-token'
        }
    }
    creds: models.Creds = CredsFactory(api_config=api, creds_details=creds_details)

    extractor = DefaultExtractor(api)

    assert extractor.auth == creds_details['auth']
    assert extractor.headers['Cookie'] == 'my-cookie=my-value'
    assert extractor.auth_token == creds_details['auth_token']
    assert extractor.headers == {
        'Cookie': 'my-cookie=my-value',
        'Token': 'my-token'
    }


def test_source_target_path():

    download_dir = '/path/to/download'
    source_url = 'http://path/to/source'
    api: models.API = APIFactory(name='my nice api')
    source: models.Source = SourceFactory(base_url=source_url, data_dir=download_dir, api_config=api)

    cached_file1: models.CachedFile = CachedFileFactory(url='path/to/file1',
                                                        path='path/to/file1/on/disk', source=source)
    cached_file2: models.CachedFile = CachedFileFactory(url='path/to/file2', source=source)

    extractor = DefaultExtractor(api)

    expected_source_targets = [
        SourceTargetPair(cached_file1, Path(download_dir).resolve() / 'path/to/file1/on/disk'),
        SourceTargetPair(cached_file2, Path(download_dir).resolve() / 'file2')
    ]

    assert expected_source_targets == extractor.source_target_list


def test_ftp_writer():

    target = io.BytesIO()

    bar = tqdm(1)
    DefaultExtractor._ftp_writer(target, b'hello world', bar=bar)
    target.seek(0)
    bar.close()

    assert target.read() == b'hello world'


def test_generic_writer():

    size = 32768
    bar = tqdm(size)
    letters = [chr(i) for i in range(65, 65 + 26)]
    data = bytes(''.join(random.choices(letters, k=size)), 'utf-8')

    source = io.BytesIO(data)
    source.seek(0)
    target = io.BytesIO()

    api: models.API = APIFactory(name='my nice api')
    extractor = DefaultExtractor(api)
    extractor._generic_writer(source, target, bar=bar)

    source.seek(0)
    target.seek(0)
    assert source.read() == target.read()


def test_requires_update(tmp_path):

    api: models.API = APIFactory(name='my nice api')
    extractor = DefaultExtractor(api)

    assert extractor._requires_update(Path('some/file'), 1, timedelta(days=1))

    tf = NamedTemporaryFile(dir=tmp_path, delete=False)
    tf.write(b'hello world')
    tf.close()

    assert not extractor._requires_update(Path(tf.name), 11)
    assert not extractor._requires_update(Path(tf.name), -1, timedelta(days=1))
    assert extractor._requires_update(Path(tf.name), -1, timedelta(0))


def test_update_cache_file(session, tmp_path):

    tf = NamedTemporaryFile(dir=tmp_path, delete=False)
    tf.write(b'hello world')
    tf.close()

    api: models.API = APIFactory(name='my nice api')
    extractor = DefaultExtractor(api)

    cached_file: models.CachedFile = CachedFileFactory(path=tf.name)
    extractor._update_file_cache(cached_file, Path(tf.name))

    cached_file = session.query(models.CachedFile).get(cached_file.id)

    assert cached_file.path == tf.name
    assert cached_file.hash == file_hash(Path(tf.name)).hexdigest()
    assert cached_file.last_download is not None
    assert cached_file.size == 11


@mock.patch('ketl.extractor.Extractor.DefaultExtractor.get_file')
@mock.patch('ketl.extractor.Extractor.DefaultExtractor.source_target_list', new_callable=mock.PropertyMock)
def test_extract(mock_source_files_list, mock_get_file):

    api: models.API = APIFactory()
    source: models.Source = SourceFactory(api_config=api, data_dir='data/dir')

    extractor = DefaultExtractor(api)

    cached_file1: models.CachedFile = CachedFileFactory(url='url/to/file1', path='path/to/file1',
                                                        expected_mode=models.ExpectedMode.self,
                                                        source=source)
    cached_file2: models.CachedFile = CachedFileFactory(url='url/to/file2', path='path/to/file2',
                                                        expected_mode=models.ExpectedMode.self,
                                                        source=source)
    cached_file3: models.CachedFile = CachedFileFactory(url='url/to/file3', path='path/to/file3',
                                                        expected_mode=models.ExpectedMode.self,
                                                        source=source)

    mock_source_files_list.return_value = [
        SourceTargetPair(cached_file1, Path('path/to/file1')),
        SourceTargetPair(cached_file2, Path('path/to/file2')),
        SourceTargetPair(cached_file3, Path('path/to/file3')),
    ]

    mock_get_file.side_effect = [cached_file1, cached_file2, None]

    expected_files = extractor.extract()

    for i, ef in enumerate(expected_files):
        assert str(ef) == f'data/dir/path/to/file{i + 1}'


@mock.patch('ketl.extractor.Extractor.FTP')
@mock.patch('ketl.extractor.Extractor.DefaultExtractor._requires_update')
def test_fetch_ftp(mock_requires_update, mock_ftp_class, tmp_path):

    mock_requires_update.return_value = True

    mock_ftp = mock.Mock()
    mock_ftp.login.return_value = None
    mock_ftp.size.return_value = 11
    mock_ftp.retrbinary.return_value = None

    mock_ftp_class.return_value = mock_ftp

    api: models.API = APIFactory(name='my nice api')
    extractor = DefaultExtractor(api)

    cached_file: models.CachedFile = CachedFileFactory(url='url/to/file')

    with NamedTemporaryFile(dir=tmp_path) as tf:
        result = extractor._fetch_ftp_file(cached_file, Path(tf.name), tqdm(1))

    assert result

    mock_requires_update.return_value = False
    assert not extractor._fetch_ftp_file(cached_file, Path(tf.name), tqdm(1))


@mock.patch('ketl.extractor.Extractor.smart_open')
@mock.patch('ketl.extractor.Extractor.DefaultExtractor._requires_update')
@mock.patch('ketl.extractor.Extractor.DefaultExtractor._generic_writer')
def test_fetch_generic_file(mock_generic_writer, mock_requires_update, mock_smart_open, tmp_path):

    mock_requires_update.return_value = True

    mock_open_file = mock.Mock()
    mock_open_file.content_length = 1

    mock_smart_open.return_value.__enter__.return_value = mock_open_file

    mock_generic_writer.return_value = None

    api: models.API = APIFactory(name='my nice api')
    extractor = DefaultExtractor(api)

    cached_file: models.CachedFile = CachedFileFactory(url='url/to/file', url_params={'query': 'item'})

    with NamedTemporaryFile(dir=tmp_path) as tf:
        result = extractor._fetch_generic_file(cached_file, Path(tf.name),
                                               headers={'Bearer': 'Token'},
                                               auth={'username': 'user', 'password': 'password'})

    assert result

    mock_requires_update.return_value = False
    assert not extractor._fetch_generic_file(cached_file, Path(tf.name))


@mock.patch('ketl.extractor.Extractor.DefaultExtractor._update_file_cache')
@mock.patch('ketl.extractor.Extractor.DefaultExtractor._fetch_generic_file')
@mock.patch('ketl.extractor.Extractor.DefaultExtractor._fetch_ftp_file')
def test_get_file(mock_fetch_ftp, mock_fetch_generic, mock_update):

    mock_update.return_value = None

    api = APIFactory(name='my nice api')
    source1: models.Source = SourceFactory(base_url='ftp://base/url')
    source2: models.Source = SourceFactory(base_url='http://base/url')
    cached_file: models.CachedFile = CachedFileFactory(url='file', source=source1, url_params={'query': 'item'})

    extractor = DefaultExtractor(api)

    mock_fetch_ftp.return_value = True
    assert extractor.get_file(cached_file, Path('target')) == cached_file

    cached_file: models.CachedFile = CachedFileFactory(url='http://url/to/file', source=source2,
                                                       url_params={'query': 'item'})

    mock_fetch_generic.return_value = True
    assert extractor.get_file(cached_file, Path('target')) == cached_file

    mock_fetch_generic.return_value = False
    assert extractor.get_file(cached_file, Path('target')) is None

    mock_fetch_generic.side_effect = [ValueError('something bad happened')]
    mock_fetch_generic.return_value = True
    assert extractor.get_file(cached_file, Path('target')) is None
