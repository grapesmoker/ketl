#!/usr/bin/env python3
"""Console script for ketl."""
import sys
import os
import click
import alembic.config

from alembic.config import Config, command
from pathlib import Path
from ketl.db import settings


@click.group()
def ketl():

    pass


@ketl.command()
@click.option('--db-dsn')
def apply_migrations(db_dsn=None):

    db_dsn = db_dsn or settings.DB_DSN

    current_dir = Path(__name__).parent
    config_dir = Path(current_dir) / 'ketl' / 'db' / 'config'

    alembic_cfg = Config(file_=str(config_dir / 'alembic.ini'))
    alembic_cfg.set_main_option('script_location', str(config_dir))
    alembic_cfg.set_main_option('sqlalchemy.url', db_dsn)
    command.upgrade(alembic_cfg, 'head')


if __name__ == "__main__":
    sys.exit(ketl())  # pragma: no cover
