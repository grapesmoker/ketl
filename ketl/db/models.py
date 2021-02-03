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
from typing import Optional, Set, List, Dict, Type
from marshmallow import Schema
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column, Boolean, Integer, String, ForeignKey, DateTime,
    JSON, Enum, Interval, UniqueConstraint
)
from sqlalchemy.orm import relationship

from ketl.extractor.Rest import RestMixin
from ketl.utils.file_utils import file_hash
from ketl.utils.db_utils import get_or_create
from ketl.db.settings import get_engine, get_session


Base = declarative_base()


class API(Base, RestMixin):

    __tablename__ = 'api_config'

    id = Column(Integer, primary_key=True)
    name = Column(String, index=True, unique=True)
    description = Column(String, index=True)
    sources = relationship('Source', back_populates='api_config', lazy='joined', enable_typechecks=False)
    creds = relationship('Creds', back_populates='api_config', lazy='joined', uselist=False)
    hash = Column(String)

    @abstractmethod
    def setup(self):

        if not self.name:
            self.name = self.__class__.__name__

        session = get_session()
        existing_api = session.query(API).filter(API.name == self.name).one_or_none()
        if not existing_api:
            session.add(self)
            session.commit()
        else:
            self.id = existing_api.id

    def api_hash(self):

        s = sha1(bytes(self.name, 'utf-8'))
        for source in self.sources:
            s.update(source.source_hash.digest())

        return s.hexdigest()

    @staticmethod
    def get_instance(model: Type['API'], name=None):

        name = name or model.__name__
        instance, created = get_or_create(model, name=name)
        return instance

    @property
    def expected_files(self):
        return [expected_file for source in self.sources
                for cached_file in source.source_files
                for expected_file in cached_file.expected_files]


class ExpectedMode(enum.Enum):

    auto = 'auto'
    explicit = 'explicit'
    self = 'self'


class CachedFile(Base):

    BLOCK_SIZE = 65536

    __tablename__ = 'cached_file'

    __table_args__ = (
        UniqueConstraint('source_id', 'url', 'path'),
    )

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
    meta = Column(JSON, nullable=True)

    @property
    def full_path(self) -> Path:
        return Path(self.source.data_dir).resolve() / self.path

    @property
    def file_hash(self):

        if self.path:
            path = Path(self.path).resolve()
            if path.exists() and not path.is_dir():
                return file_hash(path, self.BLOCK_SIZE)

        return sha1()

    def uncompress(self):

        extract_dir = Path(self.extract_to) if self.extract_to is not None and self.extract_to != '' else Path('.')

        if self.is_archive:
            expected_paths: Set[Path] = {Path(file.path) for file in self.expected_files}
            if tarfile.is_tarfile(self.path):
                self._extract_tar(extract_dir, expected_paths)
            elif zipfile.is_zipfile(self.path):
                self._extract_zip(extract_dir, expected_paths)
            elif self.path.endswith('.gz'):
                self._extract_gzip(extract_dir, expected_paths)
            elif Path(self.path).suffix in ['.xz', '.lz', '.lzma']:
                self._extract_lzma(extract_dir, expected_paths)
        elif self.expected_mode == ExpectedMode.self:
            expected_file = ExpectedFile(cached_file=self, path=str(Path(self.source.data_dir) / self.path))
            session = get_session()
            session.add(expected_file)
            session.commit()

    def _extract_tar(self, extract_dir: Path, expected_paths: Set[Path]):
        tf = tarfile.open(self.path)
        archived_paths = {Path(file) for file in tf.getnames()}
        if self.expected_mode == ExpectedMode.auto:
            self._generate_expected_files(extract_dir, archived_paths, expected_paths)
            tf.extractall(path=extract_dir)
        elif self.expected_mode == ExpectedMode.explicit:
            target_paths = set(map(lambda p: (extract_dir / p.name).resolve(), expected_paths))
            for path in archived_paths:
                if path.is_absolute() or str(path).startswith('..'):
                    raise ValueError(f'Invalid path present in archive: {path}. '
                                     f'Paths must not be absolute or begin with ..')
                elif (extract_dir / path.name).resolve() in target_paths:
                    target = extract_dir / path
                    with open(target, 'wb') as target_file:
                        with tf.extractfile(str(path)) as source_file:
                            shutil.copyfileobj(source_file, target_file)

    def _extract_zip(self, extract_dir: Path, expected_paths: Set[Path]):
        zf = zipfile.ZipFile(self.path)
        archived_paths = {Path(file) for file in zf.namelist()}
        if self.expected_mode == ExpectedMode.auto:
            self._generate_expected_files(extract_dir, archived_paths, expected_paths)
            zf.extractall(path=extract_dir)
        elif self.expected_mode == ExpectedMode.explicit:
            target_paths = set(map(lambda p: (extract_dir / p.name).resolve(), expected_paths))
            for path in archived_paths:
                if (extract_dir / path.name).resolve() in target_paths:
                    zf.extract(str(path), path=extract_dir)

    def _extract_gzip(self, extract_dir: Path, expected_paths: Set[Path]):
        result_file = extract_dir / Path(self.path).stem
        if result_file in expected_paths:
            with open(result_file, 'wb') as target:
                with gzip.open(self.path, 'r') as source:
                    shutil.copyfileobj(source, target)

    def _extract_lzma(self, extract_dir: Path, expected_paths: Set[Path]):
        result_file = extract_dir / Path(self.path).stem
        if result_file in expected_paths:
            with open(result_file, 'wb') as target:
                with lzma.open(self.path, 'r') as source:
                    shutil.copyfileobj(source, target)

    def _generate_expected_files(self, extract_dir: Path, archived_paths: Set[Path], expected_paths: Set[Path]) -> None:

        session = get_session()

        missing_paths = {path for path in archived_paths if extract_dir / path not in expected_paths}
        for path in missing_paths:
            expected_file = ExpectedFile(path=str(extract_dir / path), cached_file=self)
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

    __table_args__ = (
        UniqueConstraint('base_url', 'data_dir', 'api_config_id'),
    )

    id = Column(Integer, primary_key=True)
    source_type = Column(String, index=True)
    base_url = Column(String, index=True)
    data_dir = Column(String, index=True)
    api_config_id = Column(Integer, ForeignKey('api_config.id', ondelete='CASCADE'))
    api_config = relationship('API', back_populates='sources', enable_typechecks=False)
    source_files = relationship('CachedFile', back_populates='source',
                                cascade='all, delete-orphan',
                                passive_deletes=True,
                                lazy='joined')

    __mapper_args__ = {
        'polymorphic_identity': 'source',
        'polymorphic_on': source_type
    }

    @property
    def expected_files(self):
        return [expected_file for cached_file in self.source_files
                for expected_file in cached_file.expected_files]

    @property
    def source_hash(self):

        s = sha1(bytes(self.base_url + self.data_dir, 'utf-8'))
        for source_file in self.source_files:
            s.update(source_file.file_hash.digest())

        return s


class ExpectedFile(Base):

    __tablename__ = 'expected_file'

    __table_args__ = (
        UniqueConstraint('path', 'cached_file_id'),
    )

    BLOCK_SIZE = 65536

    id = Column(Integer, primary_key=True)
    path = Column(String, index=True)
    hash = Column(String)
    size = Column(Integer, index=True)
    cached_file_id = Column(Integer, ForeignKey('cached_file.id', ondelete='CASCADE'))
    cached_file = relationship('CachedFile', back_populates='expected_files')

    @property
    def file_hash(self):

        if self.path:
            path = Path(self.path).resolve()
            if path.exists() and not path.is_dir():
                return file_hash(path, self.BLOCK_SIZE)

        return sha1()

