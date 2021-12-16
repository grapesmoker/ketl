import datetime

import pytest
import io
import pandas as pd
import random

from datetime import timedelta
from tqdm import tqdm
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest import mock
from hashlib import sha1
from furl import furl

from ketl.db import models
from ketl.extractor.Extractor import BaseExtractor, DefaultExtractor
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

    assert extractor.auth == creds.creds_details['auth']
    assert extractor.headers['Cookie'] == 'my-cookie=my-value'
    assert extractor.auth_token == creds.creds_details['auth_token']
    assert extractor.headers == {
        'Cookie': 'my-cookie=my-value',
        'Token': 'my-token'
    }

    extractor_with_id = DefaultExtractor(api.id)
    extractor_with_name = DefaultExtractor(api.name)
    assert extractor_with_id.api == extractor.api == extractor_with_name.api


def test_handle_s3_urls():

    api = APIFactory(name='my nice api')
    extractor = DefaultExtractor(api)

    bad_s3_url = 's3://some-bucket/badly#formed-file'
    url = furl(bad_s3_url)

    result = extractor._handle_s3_urls(url)

    assert result == 's3://some-bucket/badly%23formed-file'

    bad_s3_url = 's3://some-bucket/badly&formed-file'
    url = furl(bad_s3_url)

    result = extractor._handle_s3_urls(url)

    assert result == 's3://some-bucket/badly%26formed-file'


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


@mock.patch('ketl.extractor.Extractor.Pool')
@mock.patch('ketl.extractor.Extractor.DefaultExtractor.get_file')
def test_extract(mock_get_file, mock_pool):

    api: models.API = APIFactory()
    source: models.Source = SourceFactory(api_config=api, data_dir='data/dir')

    extractor = DefaultExtractor(api)

    cached_file1: models.CachedFile = CachedFileFactory(url='url/to/file1', path='path/to/file1',
                                                        expected_mode=models.ExpectedMode.self,
                                                        hash='hash1',
                                                        source=source)
    cached_file2: models.CachedFile = CachedFileFactory(url='url/to/file2', path='path/to/file2',
                                                        expected_mode=models.ExpectedMode.self,
                                                        source=source)
    cached_file3: models.CachedFile = CachedFileFactory(url='url/to/file3', path='path/to/file3',
                                                        expected_mode=models.ExpectedMode.self,
                                                        source=source)

    mock_get_file.side_effect = [
        {'id': cached_file1.id, 'hash': 'hash1', 'last_download': datetime.datetime.now(), 'size': 1},
        {'id': cached_file2.id, 'hash': 'hash2', 'last_download': datetime.datetime.now(), 'size': 1},
        None
    ]

    expected_files = extractor.extract()

    for i, ef in enumerate(expected_files):
        assert f'data/dir/path/to/file{i + 1}' in str(ef)

    # test with only missing files
    extractor = DefaultExtractor(api, skip_existing_files=True, on_disk_check='hash')

    mock_get_file.side_effect = [
        {'id': cached_file1.id, 'hash': 'hash1', 'last_download': datetime.datetime.now(), 'size': 1},
        {'id': cached_file2.id, 'hash': 'hash2', 'last_download': datetime.datetime.now(), 'size': 1},
        None
    ]

    expected_files = extractor.extract()

    for i, ef in enumerate(expected_files):
        assert f'data/dir/path/to/file{i + 1}' in str(ef)

    # test with multiprocessing

    mock_future1 = mock.Mock()
    mock_future2 = mock.Mock()
    mock_future3 = mock.Mock()

    mock_future1.get.return_value = {
        'id': cached_file1.id, 'hash': 'hash1', 'last_download': datetime.datetime.now(), 'size': 1
    }
    mock_future2.get.return_value = {
        'id': cached_file2.id, 'hash': 'hash2', 'last_download': datetime.datetime.now(), 'size': 1
    }
    mock_future3.get.return_value = None

    mock_futures = mock.Mock()
    mock_futures.get.return_value = [mock_future1, mock_future2, mock_future3]

    mock_mp_pool = mock.Mock()
    mock_mp_pool.starmap_async.return_value = mock_futures
    mock_mp_pool.join.return_value = None
    mock_pool.__enter__.return_value = mock_mp_pool

    extractor = DefaultExtractor(api, concurrency='multiprocess')

    mock_get_file.side_effect = [
        {'id': cached_file1.id, 'hash': 'hash1', 'last_download': datetime.datetime.now(), 'size': 1},
        {'id': cached_file2.id, 'hash': 'hash2', 'last_download': datetime.datetime.now(), 'size': 1},
        None
    ]

    expected_files = extractor.extract()

    for i, ef in enumerate(expected_files):
        assert f'data/dir/path/to/file{i + 1}' in str(ef)


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
        result = extractor._fetch_ftp_file(cached_file.full_url, Path(tf.name), timedelta(days=7))

    assert result

    mock_requires_update.return_value = False
    assert not extractor._fetch_ftp_file(cached_file.full_url, Path(tf.name), timedelta(days=7))


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
        result = extractor._fetch_generic_file(cached_file.full_url, Path(tf.name),
                                               timedelta(days=7),
                                               url_params=cached_file.url_params,
                                               headers={'Bearer': 'Token'},
                                               auth={'username': 'user', 'password': 'password'})

    assert result

    mock_requires_update.return_value = False
    assert not extractor._fetch_generic_file(cached_file.full_url, Path(tf.name), timedelta(days=7))


@mock.patch('ketl.extractor.Extractor.Path.stat')
@mock.patch('ketl.extractor.Extractor.file_hash')
@mock.patch('ketl.extractor.Extractor.DefaultExtractor._update_file_cache')
@mock.patch('ketl.extractor.Extractor.DefaultExtractor._fetch_generic_file')
@mock.patch('ketl.extractor.Extractor.DefaultExtractor._fetch_ftp_file')
def test_get_file(mock_fetch_ftp, mock_fetch_generic, mock_update, mock_hash, mock_stat):

    mock_update.return_value = None
    mock_hash.side_effect = [sha1(b'hash1'), sha1(b'hash2'), sha1(b'hash3')]
    mock_stat_result = mock.Mock()
    mock_stat_result.st_size = 1024
    mock_stat.side_effect = [mock_stat_result, mock_stat_result, mock_stat_result]

    api = APIFactory(name='my nice api')
    source1: models.Source = SourceFactory(base_url='ftp://base/url', data_dir='data_dir')
    source2: models.Source = SourceFactory(base_url='http://base/url', data_dir='data_dir')
    cached_file: models.CachedFile = CachedFileFactory(
        url='file', source=source1, path='cached_file', url_params={'query': 'item'})

    extractor = DefaultExtractor(api)

    interval = datetime.timedelta(days=7)

    mock_fetch_ftp.return_value = True
    result = extractor.get_file(
        cached_file.id, cached_file.full_url, cached_file.full_path, interval
    )
    # can't patch datetime
    assert isinstance(result['last_download'], datetime.datetime)
    del result['last_download']
    assert result == {
        'hash': sha1(b'hash1').hexdigest(),
        'id': 1,
        'size': 1024
    }

    cached_file: models.CachedFile = CachedFileFactory(
        url='file', source=source2, path='cached_file', url_params={'query': 'item'})

    mock_fetch_generic.return_value = True
    result = extractor.get_file(
        cached_file.id, cached_file.full_url, cached_file.full_path, interval
    )
    # can't patch datetime
    assert isinstance(result['last_download'], datetime.datetime)
    del result['last_download']
    assert result == {
        'hash': sha1(b'hash2').hexdigest(),
        'id': 2,
        'size': 1024
    }

    mock_fetch_generic.return_value = False
    assert extractor.get_file(
        cached_file.id, cached_file.full_url, cached_file.full_path, interval
    ) is None

    mock_fetch_generic.side_effect = [ValueError('something bad happened')]
    mock_fetch_generic.return_value = True
    assert extractor.get_file(
        cached_file.id, cached_file.full_url, cached_file.full_path, interval
    ) is None
