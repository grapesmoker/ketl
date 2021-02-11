#!/usr/bin/env python3
"""Console script for ketl."""
import sys
import click
import alembic.config

from ketl.db import settings


@click.group()
@click.pass_context
def ketl(ctx):

    ctx.ensure_object(dict)

    import os
    print(os.getcwd())


@ketl.command()
@click.option('--db-dsn')
@click.pass_context
def apply_migrations(ctx, db_dsn):

    settings.DB_DSN = db_dsn
    import os
    print(os.getcwd())


if __name__ == "__main__":
    sys.exit(ketl())  # pragma: no cover
