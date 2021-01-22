import factory
from factory.alchemy import SQLAlchemyModelFactory

from ketl.db.settings import get_session
from ketl.db import models


class APIFactory(SQLAlchemyModelFactory):

    class Meta:
        model = models.API
        sqlalchemy_session = get_session()

    name = factory.Sequence(lambda n: f'api_{n}')
    sources = factory.RelatedFactory(
        'tests.factories.SourceFactory',
        factory_related_name='api_config',
    )


class SourceFactory(SQLAlchemyModelFactory):

    class Meta:
        model = models.Source
        sqlalchemy_session = get_session()

    api_config = factory.SubFactory(APIFactory)


class CachedFileFactory(SQLAlchemyModelFactory):

    class Meta:
        model = models.CachedFile
        sqlalchemy_session = get_session()

    source = factory.SubFactory(SourceFactory)


class CredsFactory(SQLAlchemyModelFactory):

    class Meta:
        model = models.Creds
        sqlalchemy_session = get_session()

    api_config = factory.SubFactory(APIFactory)


class ExpectedFileFactory(SQLAlchemyModelFactory):

    class Meta:
        model = models.ExpectedFile
        sqlalchemy_session = get_session()

    cached_file = factory.SubFactory(CachedFileFactory)
