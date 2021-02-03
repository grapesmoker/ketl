from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import IntegrityError

from ketl.db.settings import get_session


def get_or_create(model, **kwargs):

    session = get_session()
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

