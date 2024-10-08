from typing import Any

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import IntegrityError
from sqlalchemy import MetaData

from ketl.db.settings import get_session


def get_or_create(model, session=None, **kwargs) -> tuple[Any, bool]:
    """
    Get or create a model based on its properties.

    The model is returned along with flag indicating whether it was created
    or not.
    :param model: A sqlachemy model.
    :param session: The session to use. A session will be retrieved if one is
    not provided.
    :param kwargs: kwargs to be passed to the model instantiation.
    :return: A tuple consisting of the model and a flag indicationg whether the
    model was created.
    """
    session = session or get_session()
    try:
        return session.query(model).filter_by(**kwargs).one(), False
    except NoResultFound:
        created = model(**kwargs)
        try:
            session.add(created)
            session.commit()
            return created, True
        except IntegrityError:
            session.rollback()
            return session.query(model).filter_by(**kwargs).one(), False


def merge_meta(*metas) -> MetaData:
    merged = MetaData()
    for meta in metas:
        for table in meta.tables.values():
            table.tometadata(merged)
    return merged
