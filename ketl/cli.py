#!/usr/bin/env python3
"""Console script for ketl."""
import sys
import click

from ketl.db import settings
from ketl.db.models import Base


@click.group()
def ketl():

    pass


@ketl.command()
@click.option('--db-dsn')
def create_tables(db_dsn=None):

    db_dsn = db_dsn or settings.DB_DSN

    engine = settings.get_engine(db_dsn)
    Base.metadata.create_all(engine)


if __name__ == "__main__":
    sys.exit(ketl())  # pragma: no cover
