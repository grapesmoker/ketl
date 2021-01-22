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


def test_expected_file_hash(session, temp_dir):

    expected_file: models.ExpectedFile = ExpectedFileFactory(path='/path/to/nonexistent/file')
    default_hash = expected_file.file_hash
    assert default_hash is not None

    with NamedTemporaryFile(dir=temp_dir) as tf:
        tf.write(b'hello world')

        expected_file: models.ExpectedFile = ExpectedFileFactory(path=tf.name)
        assert expected_file.file_hash != default_hash
        assert len(expected_file.file_hash.hexdigest()) > 0


def test_cached_file_cache(session, temp_dir):

    missing_cached_file: models.CachedFile = CachedFileFactory(path='/path/to/missing/file')
    assert missing_cached_file.file_hash.hexdigest() == sha1().hexdigest()

    with NamedTemporaryFile(dir=temp_dir, delete=False) as tf:
        tf.write(b'hello world - cached file')
        tf.close()

        cached_file: models.CachedFile = CachedFileFactory(path=tf.name)

        cached_file_hash = cached_file.file_hash

        assert cached_file_hash.hexdigest() != sha1().hexdigest()


def test_source_hash(session, temp_dir):

    source: models.Source = SourceFactory(base_url='http://base.url', data_dir='/download/dir')
    source_hash = source.source_hash.hexdigest()
    assert source_hash == sha1(bytes(source.base_url + source.data_dir, 'utf-8')).hexdigest()

    with NamedTemporaryFile(dir=temp_dir, delete=False) as tf:
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


def test_cached_file_extract_gzip(temp_dir):

    tf = NamedTemporaryFile(dir=temp_dir, delete=False)
    tf.write(b'hello world - gzip')
    tf.close()

    raw_file = Path(tf.name)
    compressed_file = Path(temp_dir) / 'test.gz'
    result_file = Path(temp_dir) / 'test'

    with open(raw_file, 'rb') as tf:
        with gzip.open(compressed_file, mode='wb') as cf:
            shutil.copyfileobj(tf, cf)

    cached_file: models.CachedFile = CachedFileFactory(path=str(compressed_file), is_archive=True, extract_to=temp_dir)
    expected_file: models.ExpectedFile = ExpectedFileFactory(path=str(result_file), cached_file=cached_file)
    cached_file.uncompress()

    assert Path(expected_file.path).exists()

    with open(expected_file.path, 'rb') as f:
        data = f.read()
        assert data == b'hello world - gzip'


def test_cached_file_extract_lzma(temp_dir):

    tf = NamedTemporaryFile(dir=temp_dir, delete=False)
    tf.write(b'hello world - lzma')
    tf.close()

    raw_file = Path(tf.name)
    compressed_file = Path(temp_dir) / 'test.xz'
    result_file = Path(temp_dir) / 'test'

    with open(raw_file, 'rb') as tf:
        with lzma.open(compressed_file, mode='wb') as cf:
            shutil.copyfileobj(tf, cf)

    cached_file: models.CachedFile = CachedFileFactory(path=str(compressed_file), is_archive=True, extract_to=temp_dir)
    expected_file: models.ExpectedFile = ExpectedFileFactory(path=str(result_file), cached_file=cached_file)
    cached_file.uncompress()

    assert Path(expected_file.path).exists()

    with open(expected_file.path, 'rb') as f:
        data = f.read()
        assert data == b'hello world - lzma'


def test_cached_file_extract_zip(temp_dir):

    temp_path = Path(temp_dir)

    tf1 = NamedTemporaryFile(dir=temp_dir, delete=False)
    tf1.write(b'expected file')
    tf1.close()
    tf1_path = temp_path / Path(tf1.name).name

    tf2 = NamedTemporaryFile(dir=temp_dir, delete=False)
    tf2.write(b'not expected file')
    tf2.close()
    tf2_path = temp_path / Path(tf2.name).name

    compressed_file = Path(temp_dir) / 'test.zip'

    with zipfile.ZipFile(compressed_file, mode='w') as zf:
        zf.write(tf1_path, arcname=tf1_path.name)
        zf.write(tf2_path, arcname=tf2_path.name)

    tf1_path.unlink()
    tf2_path.unlink()

    cached_file: models.CachedFile = CachedFileFactory(path=str(compressed_file), is_archive=True, extract_to=temp_dir,
                                                       expected_mode=models.ExpectedMode.explicit)
    expected_file: models.ExpectedFile = ExpectedFileFactory(path=str(tf1_path), cached_file=cached_file)

    cached_file.uncompress()

    assert Path(expected_file.path).exists()
    assert not Path(tf2.name).exists()

    with open(expected_file.path, 'rb') as f:
        data = f.read()
        assert data == b'expected file'

    Path(expected_file.path).unlink()

    cached_file.expected_mode = models.ExpectedMode.auto
    cached_file.uncompress()

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


def test_cached_file_extract_tar(session, temp_dir):

    temp_path = Path(temp_dir)

    tf1 = NamedTemporaryFile(dir=temp_dir, delete=False)
    tf1.write(b'expected file')
    tf1.close()
    tf1_path = temp_path / Path(tf1.name).name

    tf2 = NamedTemporaryFile(dir=temp_dir, delete=False)
    tf2.write(b'not expected file')
    tf2.close()
    tf2_path = temp_path / Path(tf2.name).name

    compressed_file = Path(temp_dir) / 'test.tar.gz'

    with tarfile.open(compressed_file, mode='w:gz') as cf:
        cf.add(tf1_path, arcname=tf1_path.name)
        cf.add(tf2_path, arcname=tf2_path.name)

    tf1_path.unlink()
    tf2_path.unlink()

    cached_file: models.CachedFile = CachedFileFactory(path=str(compressed_file), is_archive=True, extract_to=temp_dir,
                                                       expected_mode=models.ExpectedMode.explicit)
    expected_file: models.ExpectedFile = ExpectedFileFactory(path=str(tf1_path), cached_file=cached_file)

    cached_file.uncompress()

    assert Path(expected_file.path).exists()
    assert not Path(tf2.name).exists()

    with open(expected_file.path, 'rb') as f:
        data = f.read()
        assert data == b'expected file'

    Path(expected_file.path).unlink()

    cached_file.expected_mode = models.ExpectedMode.auto
    cached_file.uncompress()

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
