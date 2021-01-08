from hashlib import sha1
from pathlib import Path
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSON

from ketl.utils.file_utils import file_hash

Base = declarative_base()


class API(Base):

    __tablename__ = 'api_config'

    id = Column(Integer, primary_key=True)
    name = Column(String, index=True)
    sources = relationship('Source', back_populates='api_config', lazy='joined')
    creds = relationship('Creds', back_populates='api_config', lazy='joined', uselist=False)
    hash = Column(String)

    def api_hash(self):

        s = sha1(bytes(self.name, 'utf-8'))
        for source in self.sources:
            s.update(source.source_hash.digest())

        return s.hexdigest()


class FileCache(Base):

    BLOCK_SIZE = 65536

    __tablename__ = 'file_cache'

    id = Column(Integer, primary_key=True)
    resource_id = Column(Integer, ForeignKey('resource.id'))
    resource = relationship('Resource')
    source_id = Column(Integer, ForeignKey('config.source.id', ondelete='CASCADE'))
    source = relationship('Source', back_populates='source_files')

    url = Column(String, index=True)
    url_params = Column(JSON)
    path = Column(String, index=True)
    last_download = Column(DateTime, nullable=True, index=True)
    last_update = Column(DateTime, nullable=True, index=True)
    hash = Column(String)
    cache_type = Column(String, index=True)
    size = Column(Integer, index=True)

    __mapper_args__ = {
        'polymorphic_identity': 'file_cache',
        'polymorphic_on': cache_type
    }

    @property
    def file_hash(self):

        if self.path:
            path = Path(self.path).resolve()
            if path.exists() and not path.is_dir():
                return file_hash(path, self.BLOCK_SIZE)
        else:
            return sha1()


class Creds(Base):

    __tablename__ = 'creds'

    id = Column(Integer, primary_key=True)
    api_config_id = Column(Integer, ForeignKey('api_config.id', ondelete='CASCADE'))
    api_config = relationship('APIConfig', back_populates='creds')
    creds_details = Column(JSON)


class Source(Base):

    __tablename__ = 'source'

    id = Column(Integer, primary_key=True)
    source_type = Column(String, index=True)
    base_url = Column(String, index=True)
    data_dir = Column(String, index=True)
    api_config_id = Column(Integer, ForeignKey('api_config.id', ondelete='CASCADE'))
    api_config = relationship('APIConfig', back_populates='sources')
    source_files = relationship('FileCache', back_populates='source',
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
    source_id = Column(Integer, ForeignKey('source.id', ondelete='CASCADE'))
    source = relationship('Source', back_populates='expected_files')

    @property
    def file_hash(self):

        s = sha1()
        if self.path:
            s.update(file_hash(Path(self.path).resolve()).digest())
        return s

