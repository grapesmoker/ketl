import requests
import enum
import tarfile
import zipfile
import gzip
import lzma
import shutil

from abc import abstractmethod
from datetime import timedelta
from hashlib import sha1
from pathlib import Path
from furl import furl
from typing import Optional, Set, List, Dict
from marshmallow import Schema
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Boolean, Integer, String, ForeignKey, DateTime, JSON, Enum, Interval
from sqlalchemy.orm import relationship

from ketl.extractor.Rest import RestMixin
from ketl.utils.file_utils import file_hash, uncompress
from ketl.db.settings import get_engine, get_session


Base = declarative_base()


class API(Base, RestMixin):

    __tablename__ = 'api_config'

    id = Column(Integer, primary_key=True)
    name = Column(String, index=True)
    sources = relationship('Source', back_populates='api_config', lazy='joined')
    creds = relationship('Creds', back_populates='api_config', lazy='joined', uselist=False)
    hash = Column(String)

    @abstractmethod
    def setup(self):
        """ Do whatever needs to be done to setup the API and get the
            relevant metadata for the files to be downloaded """
        raise NotImplementedError('setup is not implemented in the base class')

    def api_hash(self):

        s = sha1(bytes(self.name, 'utf-8'))
        for source in self.sources:
            s.update(source.source_hash.digest())

        return s.hexdigest()


class ExpectedMode(enum.Enum):
    auto = 'auto'
    explicit = 'explicit'


class CachedFile(Base):

    BLOCK_SIZE = 65536

    __tablename__ = 'cached_file'

    id = Column(Integer, primary_key=True)
    source_id = Column(Integer, ForeignKey('source.id', ondelete='CASCADE'))
    source = relationship('Source', back_populates='source_files')
    expected_files = relationship('ExpectedFile', back_populates='cached_file')
    url = Column(String, index=True)
    url_params = Column(JSON)
    path = Column(String, index=True)  # path relative to source
    last_download = Column(DateTime, nullable=True, index=True)
    last_update = Column(DateTime, nullable=True, index=True)
    refresh_interval = Column(Interval, nullable=True, index=True, default=timedelta(days=7))
    hash = Column(String, nullable=True)
    cache_type = Column(String, index=True, nullable=True)
    size = Column(Integer, index=True, nullable=True)
    is_archive = Column(Boolean, index=True, default=False)
    extract_to = Column(String, index=True, nullable=True)
    expected_mode = Column(Enum(ExpectedMode), index=True, default=ExpectedMode.explicit)
    meta = Column(JSON, index=True, nullable=True)

    @property
    def file_hash(self):

        if self.path:
            path = Path(self.path).resolve()
            if path.exists() and not path.is_dir():
                return file_hash(path, self.BLOCK_SIZE)
        else:
            return sha1()

    def uncompress(self):

        extract_dir = Path(self.extract_to) if self.extract_to is not None and self.extract_to != '' else Path('.')

        if self.is_archive:
            if tarfile.is_tarfile(self.path):
                tf = tarfile.open(self.path)
                archived_paths = {Path(file) for file in tf.getnames()}
                if self.expected_mode == ExpectedMode.auto:
                    self._generate_expected_files(archived_paths)
                    tf.extractall(path=extract_dir)
                elif self.expected_mode == ExpectedMode.explicit:
                    expected_paths: Set[Path] = {Path(file.path) for file in self.expected_files}
                    extractable_paths = expected_paths & archived_paths
                    for path in extractable_paths:
                        if path.is_absolute() or str(path).startswith('..'):
                            raise ValueError(f'Invalid path present in archive: {path}. '
                                             f'Paths must not be absolute or begin with ..')
                        target = extract_dir / path
                        with open(target, 'wb') as target_file:
                            with tf.extractfile(str(path)) as source_file:
                                shutil.copyfileobj(source_file, target_file)
            elif zipfile.is_zipfile(self.path):
                zf = zipfile.ZipFile(self.path)
                archived_paths = {Path(file) for file in zf.namelist()}
                if self.expected_mode == ExpectedMode.auto:
                    self._generate_expected_files(archived_paths)
                    zf.extractall(path=extract_dir)
                elif self.expected_mode == ExpectedMode.explicit:
                    expected_paths: Set[Path] = {Path(file.path) for file in self.expected_files}
                    extractable_paths = expected_paths & archived_paths
                    for path in extractable_paths:
                        zf.extract(str(path), path=extract_dir)
            elif self.path.endswith('.gz'):
                result_file = extract_dir / Path(self.path).stem
                with open(result_file, 'wb') as target:
                    with gzip.open(self.path, 'r') as source:
                        shutil.copyfileobj(source, target)
            elif Path(self.path).suffix in ['.xz', '.lz', '.lzma']:
                result_file = extract_dir / Path(self.path).stem
                with open(result_file, 'wb') as target:
                    with lzma.open(self.path, 'r') as source:
                        shutil.copyfileobj(source, target)

    def _generate_expected_files(self, archived_paths: Set[Path]) -> None:

        session = get_session()

        expected_paths: Set[ExpectedFile] = {Path(file.path) for file in self.expected_files}
        missing_paths = archived_paths - expected_paths
        for path in missing_paths:
            expected_file = ExpectedFile(path=path, cached_file=self)
            session.add(expected_file)
        session.commit()


class Creds(Base):

    __tablename__ = 'creds'

    id = Column(Integer, primary_key=True)
    api_config_id = Column(Integer, ForeignKey('api_config.id', ondelete='CASCADE'))
    api_config = relationship('API', back_populates='creds')
    creds_details = Column(JSON)


class Source(Base):

    __tablename__ = 'source'

    id = Column(Integer, primary_key=True)
    source_type = Column(String, index=True)
    base_url = Column(String, index=True)
    data_dir = Column(String, index=True)
    api_config_id = Column(Integer, ForeignKey('api_config.id', ondelete='CASCADE'))
    api_config = relationship('API', back_populates='sources')
    source_files = relationship('CachedFile', back_populates='source',
                                cascade='all, delete-orphan',
                                passive_deletes=True,
                                lazy='joined')
    expected_files = relationship('ExpectedFile', back_populates='source',
                                  cascade='all, delete-orphan',
                                  passive_deletes=True,
                                  lazy='joined')

    __mapper_args__ = {
        'polymorphic_identity': 'source',
        'polymorphic_on': source_type
    }

    @property
    def source_hash(self):

        s = sha1(bytes(self.base_url + self.data_dir, 'utf-8'))
        for source_file in self.source_files:
            s.update(source_file.file_hash.digest())
        for expected_file in self.expected_files:
            s.update(expected_file.file_hash.digest())

        return s


class ExpectedFile(Base):

    __tablename__ = 'expected_file'

    id = Column(Integer, primary_key=True)
    path = Column(String, index=True)
    hash = Column(String)
    size = Column(Integer, index=True)
    # source_id = Column(Integer, ForeignKey('source.id', ondelete='CASCADE'))
    # source = relationship('Source', back_populates='expected_files')
    cached_file_id = Column(Integer, ForeignKey('cached_file.id', ondelete='CASCADE'))
    cached_file = relationship('CachedFile', back_populates='expected_files')

    @property
    def file_hash(self):

        s = sha1()
        if self.path:
            s.update(file_hash(Path(self.path).resolve()).digest())
        return s


Base.metadata.create_all(get_engine())
