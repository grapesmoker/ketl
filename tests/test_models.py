import pytest
import zipfile
import gzip
import tarfile
import lzma
import shutil

from tempfile import NamedTemporaryFile, TemporaryDirectory
from pathlib import Path
from hashlib import sha1

from tests.factories import APIFactory, SourceFactory, CachedFileFactory, ExpectedFileFactory, CredsFactory

from ketl.db.settings import get_session
from ketl.db import models


def test_api_config(session):

    api = models.API(name='my nice api')
    api.setup()

    assert api.id is not None

    api = get_session().query(models.API).filter(models.API.name == 'my nice api').one()
    assert api.name == 'my nice api'

    api = models.API()
    api.setup()
    api = get_session().query(models.API).filter(models.API.name == 'API').one()
    assert api.name == 'API'
    api_id = api.id

    api = models.API()
    api.setup()
    assert api.id == api_id


def test_api_hash(tmp_path):

    api = APIFactory(name='my nice api')
    initial_api_hash = api.api_hash
    expected_api_hash = sha1(b'my nice api')

    assert initial_api_hash == expected_api_hash.hexdigest()

    source: models.Source = SourceFactory(base_url='http://base.url', data_dir='/download/dir', api_config=api)

    with NamedTemporaryFile(dir=tmp_path, delete=False) as tf:
        tf.write(b'hello world - cached file')
        tf.close()

        cached_file: models.CachedFile = CachedFileFactory(source=source, path=tf.name)

    expected_api_hash.update(source.source_hash.digest())
    api_hash = api.api_hash

    assert api_hash != initial_api_hash
    assert api_hash == expected_api_hash.hexdigest()


def test_api_get_instance(session):

    instance1 = models.API.get_instance(models.API)
    assert isinstance(instance1, models.API)
    instance2 = models.API.get_instance(models.API)
    assert isinstance(instance2, models.API)
    assert instance1 == instance2


def test_api_get_expected_files(session):

    api = APIFactory()
    source = SourceFactory(api_config=api)
    cf1 = CachedFileFactory(path='path1', source=source)
    cf2 = CachedFileFactory(path='path2', source=source)
    ef1 = ExpectedFileFactory(path='file1', cached_file=cf1)
    ef2 = ExpectedFileFactory(path='file2', cached_file=cf1)
    ef3 = ExpectedFileFactory(path='file3', cached_file=cf2)

    assert api.expected_files.all() == [ef1, ef2, ef3]
    assert source.expected_files == [ef1, ef2, ef3]


def test_expected_file_hash(session, tmp_path):

    expected_file: models.ExpectedFile = ExpectedFileFactory(path='/path/to/nonexistent/file')
    default_hash = expected_file.file_hash
    assert default_hash is not None

    with NamedTemporaryFile(dir=tmp_path) as tf:
        tf.write(b'hello world')

        expected_file: models.ExpectedFile = ExpectedFileFactory(path=tf.name)
        assert expected_file.file_hash != default_hash
        assert len(expected_file.file_hash.hexdigest()) > 0


def test_api_cached_files_on_disk(tmp_path):

    api: models.API = APIFactory(name='my nice api')

    with NamedTemporaryFile(dir=tmp_path, delete=False) as tf1:
        tf1.write(b'hello world')

    with NamedTemporaryFile(dir=tmp_path, delete=False) as tf2:
        tf2.write(b'hello world')

    with NamedTemporaryFile(dir=tmp_path, delete=False) as tf3:
        tf3.write(b'hello world')

    source = SourceFactory(data_dir=str(tmp_path), api_config=api)
    cf1 = CachedFileFactory(path=tf1.name, hash=sha1(b'hello world').hexdigest(), source=source)
    cf2 = CachedFileFactory(path=tf2.name, hash=sha1(b'hello world').hexdigest(), source=source)
    cf3 = CachedFileFactory(path=tf3.name, hash=None, source=source)
    cf4 = CachedFileFactory(path=str(tmp_path / 'missing'), hash=None, source=source)

    assert {cf.id for cf in api.cached_files_on_disk(use_hash=True, missing=True).all()} == {cf3.id, cf4.id}
    assert {cf.id for cf in api.cached_files_on_disk(use_hash=True, missing=False).all()} == {cf1.id, cf2.id}
    assert {cf.id for cf in api.cached_files_on_disk(use_hash=False, missing=False).all()} == {cf1.id, cf2.id, cf3.id}
    assert {cf.id for cf in api.cached_files_on_disk(use_hash=False, missing=True).all()} == {cf4.id}
    assert {cf.id for cf in api.cached_files_on_disk(
        use_hash=True, missing=False, limit_ids=[cf1.id]).all()} == {cf1.id}
    assert {cf.id for cf in api.cached_files_on_disk(
        use_hash=False, missing=False, limit_ids=[cf2.id]).all()} == {cf2.id}


def test_cached_file_cache(session, tmp_path):

    missing_cached_file: models.CachedFile = CachedFileFactory(path='/path/to/missing/file')
    assert missing_cached_file.file_hash.hexdigest() == sha1().hexdigest()

    with NamedTemporaryFile(dir=tmp_path, delete=False) as tf:
        tf.write(b'hello world - cached file')
        tf.close()

        cached_file: models.CachedFile = CachedFileFactory(path=tf.name)

        cached_file_hash = cached_file.file_hash

        assert cached_file_hash.hexdigest() != sha1().hexdigest()


def test_source_hash(session, tmp_path):

    source: models.Source = SourceFactory(base_url='http://base.url', data_dir='/download/dir')
    source_hash = source.source_hash.hexdigest()
    assert source_hash == sha1(bytes(source.base_url + source.data_dir, 'utf-8')).hexdigest()

    with NamedTemporaryFile(dir=tmp_path, delete=False) as tf:
        tf.write(b'hello world - cached file')
        tf.close()

        cached_file: models.CachedFile = CachedFileFactory(source=source, path=tf.name)

    expected_hash = sha1(bytes(source.base_url + source.data_dir, 'utf-8'))
    expected_hash.update(cached_file.file_hash.digest())

    source_hash_with_cached_file = source.source_hash.hexdigest()
    assert source_hash_with_cached_file != source_hash
    assert source_hash_with_cached_file == expected_hash.hexdigest()


def test_cached_file_full_path(session):

    source: models.Source = SourceFactory(base_url='http://base.url', data_dir='/download/dir')
    cached_file: models.CachedFile = CachedFileFactory(path='path/to/file', source=source)

    assert cached_file.full_path == Path('/download/dir/path/to/file')


def test_cached_file_extract_gzip(tmp_path):

    tf = NamedTemporaryFile(dir=tmp_path, delete=False)
    tf.write(b'hello world - gzip')
    tf.close()

    raw_file = Path(tf.name)
    compressed_file = Path(tmp_path) / 'test.gz'
    result_file = Path(tmp_path) / 'test'

    with open(raw_file, 'rb') as tf:
        with gzip.open(compressed_file, mode='wb') as cf:
            shutil.copyfileobj(tf, cf)

    source: models.Source = SourceFactory(data_dir='data_dir')
    cached_file: models.CachedFile = CachedFileFactory(
        path=str(compressed_file), is_archive=True, source=source, extract_to=str(tmp_path))
    expected_file: models.ExpectedFile = ExpectedFileFactory(path=str(result_file), cached_file=cached_file)
    cached_file.preprocess()

    assert Path(expected_file.path).exists()

    with open(expected_file.path, 'rb') as f:
        data = f.read()
        assert data == b'hello world - gzip'


def test_cached_file_extract_lzma(tmp_path):

    tf = NamedTemporaryFile(dir=tmp_path, delete=False)
    tf.write(b'hello world - lzma')
    tf.close()

    raw_file = Path(tf.name)
    compressed_file = Path(tmp_path) / 'test.xz'
    result_file = Path(tmp_path) / 'test'

    with open(raw_file, 'rb') as tf:
        with lzma.open(compressed_file, mode='wb') as cf:
            shutil.copyfileobj(tf, cf)

    source: models.Source = SourceFactory(data_dir='data_dir')
    cached_file: models.CachedFile = CachedFileFactory(
        path=str(compressed_file), is_archive=True, source=source, extract_to=str(tmp_path))
    expected_file: models.ExpectedFile = ExpectedFileFactory(path=str(result_file), cached_file=cached_file)
    cached_file.preprocess()

    assert Path(expected_file.path).exists()

    with open(expected_file.path, 'rb') as f:
        data = f.read()
        assert data == b'hello world - lzma'


def test_cached_file_extract_zip(tmp_path):

    temp_path = Path(tmp_path)

    tf1 = NamedTemporaryFile(dir=tmp_path, delete=False)
    tf1.write(b'expected file')
    tf1.close()
    tf1_path = temp_path / Path(tf1.name).name

    tf2 = NamedTemporaryFile(dir=tmp_path, delete=False)
    tf2.write(b'not expected file')
    tf2.close()
    tf2_path = temp_path / Path(tf2.name).name

    compressed_file = Path(tmp_path) / 'test.zip'

    with zipfile.ZipFile(compressed_file, mode='w') as zf:
        zf.write(tf1_path, arcname=tf1_path.name)
        zf.write(tf2_path, arcname=tf2_path.name)

    tf1_path.unlink()
    tf2_path.unlink()

    source: models.Source = SourceFactory(data_dir='data_dir')
    cached_file: models.CachedFile = CachedFileFactory(path=str(compressed_file), is_archive=True,
                                                       source=source,
                                                       extract_to=str(tmp_path),
                                                       expected_mode=models.ExpectedMode.explicit)
    expected_file: models.ExpectedFile = ExpectedFileFactory(
        path=str(tf1_path), cached_file=cached_file, archive_path=tf1_path.name)

    cached_file.preprocess()

    assert Path(expected_file.path).exists()
    assert not Path(tf2.name).exists()

    with open(expected_file.path, 'rb') as f:
        data = f.read()
        assert data == b'expected file'

    Path(expected_file.path).unlink()

    cached_file.expected_mode = models.ExpectedMode.auto
    cached_file.preprocess()

    assert len(cached_file.expected_files) == 2

    found1 = False
    found2 = False
    for ef in cached_file.expected_files:
        with open(ef.path, 'rb') as f:
            data = f.read()
            if data == b'expected file':
                found1 = True
            elif data == b'not expected file':
                found2 = True

    assert found1 and found2

    for ef in cached_file.expected_files:
        get_session().delete(ef)
    get_session().commit()

    cached_file.preprocess(overwrite_on_extract=False)

    assert len(cached_file.expected_files) == 2


def test_cached_file_extract_tar(session, tmp_path):

    temp_path = Path(tmp_path)

    tf1 = NamedTemporaryFile(dir=tmp_path, delete=False)
    tf1.write(b'expected file')
    tf1.close()
    tf1_path = temp_path / Path(tf1.name).name

    tf2 = NamedTemporaryFile(dir=tmp_path, delete=False)
    tf2.write(b'not expected file')
    tf2.close()
    tf2_path = temp_path / Path(tf2.name).name

    compressed_file = Path(tmp_path) / 'test.tar.gz'

    with tarfile.open(compressed_file, mode='w:gz') as cf:
        cf.add(tf1_path, arcname=tf1_path.name)
        cf.add(tf2_path, arcname=tf2_path.name)

    tf1_path.unlink()
    tf2_path.unlink()

    source: models.Source = SourceFactory(data_dir='data_dir')
    cached_file: models.CachedFile = CachedFileFactory(path=str(compressed_file), is_archive=True,
                                                       source=source,
                                                       extract_to=str(tmp_path),
                                                       expected_mode=models.ExpectedMode.explicit)
    expected_file: models.ExpectedFile = ExpectedFileFactory(
        path=str(tf1_path), cached_file=cached_file, archive_path=tf1_path.name)

    cached_file.preprocess()

    assert Path(expected_file.path).exists()
    assert not Path(tf2.name).exists()

    with open(expected_file.path, 'rb') as f:
        data = f.read()
        assert data == b'expected file'

    Path(expected_file.path).unlink()

    cached_file.expected_mode = models.ExpectedMode.auto
    cached_file.preprocess()

    assert len(cached_file.expected_files) == 2

    found1 = False
    found2 = False
    for ef in cached_file.expected_files:
        with open(ef.path, 'rb') as f:
            data = f.read()
            if data == b'expected file':
                found1 = True
            elif data == b'not expected file':
                found2 = True

    assert found1 and found2

    for ef in cached_file.expected_files:
        get_session().delete(ef)
    get_session().commit()

    cached_file.preprocess(overwrite_on_extract=False)

    assert len(cached_file.expected_files) == 2


def test_determine_target(tmp_path):

    session = get_session()
    cached_file: models.CachedFile = CachedFileFactory(path='path.tar.gz', expected_mode=models.ExpectedMode.auto)
    assert cached_file._determine_target(tmp_path) == tmp_path / 'path.tar'
    cached_file.expected_mode = models.ExpectedMode.explicit

    with pytest.raises(models.InvalidConfigurationError):
        cached_file._determine_target(tmp_path)

    expected_file1: models.ExpectedFile = ExpectedFileFactory(cached_file=cached_file,
                                                              path='path.tar')

    assert cached_file._determine_target(tmp_path) == tmp_path / expected_file1.path

    cached_file.expected_mode = models.ExpectedMode.self

    with pytest.raises(models.InvalidConfigurationError):
        cached_file._determine_target(tmp_path)

    expected_file2: models.ExpectedFile = ExpectedFileFactory(cached_file=cached_file,
                                                              path='path2.tar')

    with pytest.raises(models.InvalidConfigurationError):
        cached_file._determine_target(tmp_path)

