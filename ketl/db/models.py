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
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column, Boolean, Integer, String, ForeignKey, DateTime,
    JSON, Enum, Interval, UniqueConstraint, BigInteger, Index
)
from more_itertools import chunked
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship, joinedload, Query

from ketl.extractor.Rest import RestMixin
from ketl.utils.file_utils import file_hash
from ketl.utils.db_utils import get_or_create
from ketl.db.settings import get_engine, get_session, DB_DSN

# if we are using postgres, supply special arguments for indexing
# metadata and using JSON fields
if furl(DB_DSN).scheme == 'postgresql':
    JSON_COL = JSONB  # pragma: no cover
    index_args = dict(postgresql_using='gin')  # pragma: no cover
else:
    JSON_COL = JSON
    index_args = dict()

Base = declarative_base()


class API(Base, RestMixin):
    """ The API class is the center of the organizational model for kETL. It configures the basic logic
        of accessing some set of resources, setting up credentials as needed.
    """

    __tablename__ = 'ketl_api_config'

    id = Column(Integer, primary_key=True)
    name = Column(String, index=True, unique=True)
    description = Column(String, index=True)
    sources = relationship('Source', back_populates='api_config', enable_typechecks=False)
    creds = relationship('Creds', back_populates='api_config', lazy='joined', uselist=False)
    hash = Column(String)

    BATCH_SIZE = 10000

    @abstractmethod
    def setup(self):
        """ All subclasses of API must implement the setup method to generate the actual configuration
        that will specify what is to be downloaded.

        :return:
        """
        if not self.name:
            self.name = self.__class__.__name__

        session = get_session()
        existing_api = session.query(API).filter(API.name == self.name).one_or_none()
        if not existing_api:
            session.add(self)
            session.commit()
        else:
            self.id = existing_api.id

    @property
    def api_hash(self) -> str:
        """ Hash the API by hashing all of its sources and return the hex digest.

        :return: Hex digest of the hash.
        """

        s = sha1(bytes(self.name, 'utf-8'))
        for source in self.sources:
            s.update(source.source_hash.digest())

        return s.hexdigest()

    @staticmethod
    def get_instance(model: Type['API'], name=None) -> 'API':
        """ Retrieve an instance of the given subclass of API. There can only be one instance per name.

        :param model: A subclass of API.
        :param name: An optional name. Only one API per name is allowed.
        :return: An instance of the provided subclass of API.
        """
        name = name or model.__name__
        instance, created = get_or_create(model, name=name)
        return instance

    @property
    def expected_files(self) -> Query:
        """ Retrieve all the expected files under this API.

        :return: A list of expected files.
        """

        q = get_session().query(
            ExpectedFile
        ).join(
            CachedFile, ExpectedFile.cached_file_id == CachedFile.id
        ).join(
            Source, CachedFile.source_id == Source.id
        ).filter(
            Source.api_config_id == self.id
        )

        return q.yield_per(self.BATCH_SIZE)

    @property
    def cached_files(self) -> Query:
        """ Retrieve a list of all :class:`CachedFile` configured for this
        api and stored in the database.

        :return: a batched query object.
        """

        q: Query = get_session().query(
            CachedFile
        ).join(
            Source, CachedFile.source_id == Source.id
        ).filter(
            Source.api_config_id == self.id
        )

        return q.yield_per(self.BATCH_SIZE)

    def cached_files_on_disk(self, use_hash=True, missing=False, limit_ids=None) -> Query:
        """ Retrieve a list of all cached files thought to (or known to) be on disk.

        :param use_hash: if true, use the fact that the file has a hash in the db as evidence of existence.
            if false, actually checks whether the file is present at its path.
        :param missing: return any files that are configured but missing from disk.
        :param limit_ids: limit the result set to the specific ids supplied.
        :return: a batched query
        """

        # actually checking existence can get pretty expensive for the
        # scenario where you have lots of files on a distributed file system
        # so if we're pretty sure we haven't deleted the files manually, we
        # can trust that if a hash is present in the db, then the file exists

        files = get_session().query(
            CachedFile
        ).join(
            Source, CachedFile.source_id == Source.id
        ).filter(
            Source.api_config_id == self.id
        )

        if use_hash:
            if missing:
                files = files.filter(CachedFile.hash.is_(None))
            else:
                files = files.filter(CachedFile.hash.isnot(None))

            if limit_ids:
                files = files.filter(CachedFile.id.in_(limit_ids))

            return files.yield_per(self.BATCH_SIZE)

        else:
            q: Query = get_session().query(
                Source.data_dir, CachedFile.path, CachedFile.id
            ).join(
                Source, CachedFile.source_id == Source.id
            ).filter(
                Source.api_config_id == self.id
            )

            file_ids = set()
            for batch in chunked(q.yield_per(self.BATCH_SIZE), self.BATCH_SIZE):
                file_ids = {item[2] for item in batch if (Path(item[0]) / item[1]).resolve().exists()}

            if limit_ids:
                file_ids = file_ids & set(limit_ids)

            if missing:
                file_ids = {item[2] for item in batch} - file_ids

            return files.filter(CachedFile.id.in_(file_ids)).yield_per(self.BATCH_SIZE)


class ExpectedMode(enum.Enum):
    """ An enum representing the various ways of generating :class:`ExpectedFile` s from
    :class:`CachedFile` s.
    """

    auto = 'auto'
    explicit = 'explicit'
    self = 'self'


class InvalidConfigurationError(Exception):
    """Exception indicating an invalid configuration."""


class CachedFile(Base):
    """ The CachedFile class represents a single file that may be downloaded by an extractor.
    """

    BLOCK_SIZE = 65536

    __tablename__ = 'ketl_cached_file'

    __table_args__ = (
        UniqueConstraint('source_id', 'url', 'path'),
        Index('ix_ketl_cached_file_meta', 'meta', **index_args)
    )

    id = Column(Integer, primary_key=True)
    source_id = Column(Integer, ForeignKey('ketl_source.id', ondelete='CASCADE'))
    source = relationship('Source', back_populates='source_files')
    expected_files = relationship('ExpectedFile', back_populates='cached_file')
    url = Column(String, index=True)
    url_params = Column(JSON_COL)
    path = Column(String, index=True)  # path relative to source
    last_download = Column(DateTime, nullable=True, index=True)
    last_update = Column(DateTime, nullable=True, index=True)
    refresh_interval = Column(Interval, nullable=True, index=True, default=timedelta(days=7))
    hash = Column(String, nullable=True)
    cache_type = Column(String, index=True, nullable=True)
    size = Column(BigInteger, index=True, nullable=True)
    is_archive = Column(Boolean, index=True, default=False)
    extract_to = Column(String, index=True, nullable=True)
    expected_mode = Column(Enum(ExpectedMode), index=True, default=ExpectedMode.explicit)
    meta = Column(JSON_COL, nullable=True)

    @property
    def full_path(self) -> Path:
        """ Return the absolute path of the cached file.

        :return: The absolute path of the file.
        """
        return Path(self.source.data_dir).resolve() / self.path

    @property
    def full_url(self) -> str:

        return f'{self.source.base_url}/{self.url}'

    @property
    def file_hash(self):
        """ Return the hash of the file.

        :return: The hash object (not the digest or the hex digest!) of the file.
        """
        if self.path:
            path = Path(self.path).resolve()
            if path.exists() and not path.is_dir():
                return file_hash(path, self.BLOCK_SIZE)

        return sha1()

    def preprocess(self, overwrite_on_extract=True) -> Optional[dict]:
        """ Preprocess the file, extracting and creating expected files as needed.

        :return: Optionally returns an expected file, if one was created directly from the
            cached file. Otherwise returns None.
        """
        extract_dir = Path(self.extract_to) if self.extract_to is not None and self.extract_to != '' else Path('.')

        if self.is_archive:
            expected_paths: Set[Path] = {Path(file.path) for file in self.expected_files}
            if tarfile.is_tarfile(self.full_path):
                self._extract_tar(extract_dir, expected_paths, overwrite_on_extract=overwrite_on_extract)
            elif zipfile.is_zipfile(self.full_path):
                self._extract_zip(extract_dir, expected_paths, overwrite_on_extract=overwrite_on_extract)
            elif self.full_path.name.endswith('.gz'):
                self._extract_gzip(extract_dir)
            elif self.full_path.suffix in ['.xz', '.lz', '.lzma']:
                self._extract_lzma(extract_dir)
            return None
        elif self.expected_mode == ExpectedMode.self:
            return {'cached_file_id': self.id, 'path': str(self.full_path)}

    def _extract_tar(self, extract_dir: Path, expected_paths: Set[Path], overwrite_on_extract=True):
        """ Extracts a tarball into the target directory. Creates expected files as needed.

        :param extract_dir: The directory to which the tarball is to be extracted.
        :param expected_paths: The list of expected paths that should be generated from the archive.
        :return: None
        """
        tf = tarfile.open(self.full_path)
        if self.expected_mode == ExpectedMode.auto:
            archived_paths = {Path(file) for file in tf.getnames()}
            missing_files = self._generate_expected_files(extract_dir, archived_paths, expected_paths)
            if overwrite_on_extract:
                tf.extractall(path=extract_dir)
            else:
                tf.extractall(path=extract_dir, members=[tarfile.TarInfo(name=str(file)) for file in missing_files])
        elif self.expected_mode == ExpectedMode.explicit:
            for file in self.expected_files:
                target = extract_dir / file.path
                with open(target, 'wb') as target_file:
                    with tf.extractfile(file.archive_path) as source_file:
                        shutil.copyfileobj(source_file, target_file)

    def _extract_zip(self, extract_dir: Path, expected_paths: Set[Path], overwrite_on_extract=True):
        """ Extracts a zip archive into the target directory. Creates expected files as needed.

        :param extract_dir: The directory to which the archive is to be extracted.
        :param expected_paths: The list of expected paths that should be generated from the archive.
        :return: None
        """
        zf = zipfile.ZipFile(self.full_path)
        archived_paths = {Path(file) for file in zf.namelist()}
        if self.expected_mode == ExpectedMode.auto:
            missing_files = self._generate_expected_files(extract_dir, archived_paths, expected_paths)
            if overwrite_on_extract:
                zf.extractall(path=extract_dir)
            else:
                zf.extractall(path=extract_dir, members=[str(file) for file in missing_files])
        elif self.expected_mode == ExpectedMode.explicit:
            for file in self.expected_files:
                target = extract_dir / file.path
                info = zf.getinfo(file.archive_path)
                if not target.exists() or info.file_size != target.stat().st_size:
                    zf.extract(file.archive_path, path=extract_dir)
                    source = (extract_dir / file.archive_path).resolve()
                    if source != target.resolve():
                        shutil.move(source, target)

    def _determine_target(self, extract_dir: Path) -> Optional[Path]:
        """ Determine the target file to which a gz/lz archive is to be extracted.

        :param extract_dir: the directory to which the file should be extracted
        :return: either the full target path of the resulting file or None
        """

        if len(self.expected_files) > 1:
            raise InvalidConfigurationError(f'More than 1 expected file configured for a gz archive: {self.path}')
        if len(self.expected_files) == 0 and self.expected_mode == ExpectedMode.auto:
            return extract_dir / Path(self.path).stem
        elif len(self.expected_files) == 0 and not self.expected_mode == ExpectedMode.auto:
            raise InvalidConfigurationError(f'Expected mode is set to {self.expected_mode} but '
                                            f'no expected files supplied.')
        elif len(self.expected_files) == 1 and self.expected_mode == ExpectedMode.explicit:
            return extract_dir / self.expected_files[0].path
        else:
            raise InvalidConfigurationError('Something very bad has happened :(')

    def _extract_gzip(self, extract_dir: Path) -> None:
        """ Extracts a gz file into the target directory.

        :param extract_dir: The directory to which the archive is to be extracted.
        :return: None
        """
        result_file = self._determine_target(extract_dir)

        if result_file:
            with open(result_file, 'wb') as target:
                with gzip.open(self.full_path, 'r') as source:
                    shutil.copyfileobj(source, target)

    def _extract_lzma(self, extract_dir: Path) -> None:
        """ Extracts an lzma file into the target directory.

        :param extract_dir: The directory to which the archive is to be extracted.
        :return: None
        """
        result_file = self._determine_target(extract_dir)

        if result_file:
            with open(result_file, 'wb') as target:
                with lzma.open(self.full_path, 'r') as source:
                    shutil.copyfileobj(source, target)

    def _generate_expected_files(self, extract_dir: Path, archived_paths: Set[Path], expected_paths: Set[Path]
                                 ) -> Set[Path]:
        """ Generates expected file entries in the table if they do not already exist.

        :param extract_dir: The directory to which the archive is to be extracted.
        :param archived_paths: The list of paths contained in the archive.
        :param expected_paths: The list of expected files.
        :return: A set of paths for which expected files have been generated
        """
        session = get_session()

        missing_paths = {path for path in archived_paths if extract_dir / path not in expected_paths}
        expected_files = [ExpectedFile(path=str(extract_dir / path), cached_file=self)
                          for path in missing_paths]
        session.bulk_save_objects(expected_files)
        session.commit()

        return missing_paths


class Creds(Base):
    """ A simple class for keeping track of credentials. Details are stored in a JSON blob.

    SECURITY WARNING: creds are currently stored unencrypted. Don't put anything in here
    that requires real security.
    """

    __tablename__ = 'ketl_creds'

    id = Column(Integer, primary_key=True)
    api_config_id = Column(Integer, ForeignKey('ketl_api_config.id', ondelete='CASCADE'))
    api_config = relationship('API', back_populates='creds', enable_typechecks=False)
    creds_details = Column(JSON_COL)


class Source(Base):
    """ A class representing a source of some data. Can be subclassed on source type.
    """

    __tablename__ = 'ketl_source'

    __table_args__ = (
        UniqueConstraint('base_url', 'data_dir', 'api_config_id'),
        Index('ix_ketl_source_meta', 'meta', **index_args)
    )

    id = Column(Integer, primary_key=True)
    source_type = Column(String, index=True)
    base_url = Column(String, index=True)
    data_dir = Column(String, index=True)
    api_config_id = Column(Integer, ForeignKey('ketl_api_config.id', ondelete='CASCADE'))
    api_config = relationship('API', back_populates='sources', enable_typechecks=False)
    meta = Column(JSON_COL, nullable=True)
    source_files = relationship('CachedFile', back_populates='source',
                                cascade='all, delete-orphan',
                                passive_deletes=True,
                                lazy='joined')

    __mapper_args__ = {
        'polymorphic_identity': 'source',
        'polymorphic_on': source_type
    }

    @property
    def expected_files(self) -> List['ExpectedFile']:
        """ Return a list of expected files for the given source.

        :return: a list of :class:`ExpectedFile` s.
        """
        return [expected_file for cached_file in self.source_files
                for expected_file in cached_file.expected_files]

    @property
    def source_hash(self):

        s = sha1(bytes(self.base_url + self.data_dir, 'utf-8'))
        for source_file in self.source_files:
            s.update(source_file.file_hash.digest())

        return s


class ExpectedFile(Base):
    """ A class representing expected files to actually be processed.
    """
    __tablename__ = 'ketl_expected_file'

    __table_args__ = (
        UniqueConstraint('path', 'cached_file_id'),
        Index('ix_ketl_expected_file_meta', 'meta', **index_args)
    )

    BLOCK_SIZE = 65536

    id = Column(Integer, primary_key=True)
    path = Column(String, index=True)
    archive_path = Column(String, index=True)
    hash = Column(String)
    size = Column(BigInteger, index=True)
    cached_file_id = Column(Integer, ForeignKey('ketl_cached_file.id', ondelete='CASCADE'))
    cached_file = relationship('CachedFile', back_populates='expected_files', lazy='joined')
    processed = Column(Boolean, default=False, index=True)
    file_type = Column(String, index=True)
    last_processed = Column(DateTime, index=True)
    meta = Column(JSON_COL, nullable=True)

    @property
    def file_hash(self):
        """ Hash the expected file.

        :return: The hash object.
        """
        if self.path:
            path = Path(self.path).resolve()
            if path.exists() and not path.is_dir():
                return file_hash(path, self.BLOCK_SIZE)

        return sha1()

