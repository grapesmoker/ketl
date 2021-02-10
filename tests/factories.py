import factory
from factory.alchemy import SQLAlchemyModelFactory

from ketl.db.settings import get_session
from ketl.db import models


class APIFactory(SQLAlchemyModelFactory):

    class Meta:
        model = models.API
        sqlalchemy_session = get_session()
        sqlalchemy_session_persistence = 'commit'

    name = factory.Sequence(lambda n: f'api_{n}')


class SourceFactory(SQLAlchemyModelFactory):

    class Meta:
        model = models.Source
        sqlalchemy_session = get_session()
        sqlalchemy_session_persistence = 'commit'

    api_config = factory.SubFactory(APIFactory)


class CachedFileFactory(SQLAlchemyModelFactory):

    class Meta:
        model = models.CachedFile
        sqlalchemy_session = get_session()
        sqlalchemy_session_persistence = 'commit'

    source = factory.SubFactory(SourceFactory)


class CredsFactory(SQLAlchemyModelFactory):

    class Meta:
        model = models.Creds
        sqlalchemy_session = get_session()
        sqlalchemy_session_persistence = 'commit'

    api_config = factory.SubFactory(APIFactory)


class ExpectedFileFactory(SQLAlchemyModelFactory):

    class Meta:
        model = models.ExpectedFile
        sqlalchemy_session = get_session()
        sqlalchemy_session_persistence = 'commit'

    cached_file = factory.SubFactory(CachedFileFactory)
