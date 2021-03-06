"""test

Revision ID: ef65c717d766
Revises:
Create Date: 2021-02-18 11:20:13.238002

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql
from alembic import context
from furl import furl
# revision identifiers, used by Alembic.
revision = 'ef65c717d766'
down_revision = None
branch_labels = None
depends_on = None

engine_url = furl(context.get_bind().engine.url)
print(engine_url)
use_postgres = False
if furl.scheme == 'postgresql':
    JSON_COL = postgresql.JSONB
    col_args = {'astext_type': sa.Text(), 'nullable': True}
    index_args = {'postgresql_using': 'gin'}
    use_postgres = True
else:
    JSON_COL = sa.JSON
    index_args = {}
    col_args = {}


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('ketl_api_config',
                    sa.Column('id', sa.Integer(), nullable=False),
                    sa.Column('name', sa.String(), nullable=True),
                    sa.Column('description', sa.String(), nullable=True),
                    sa.Column('hash', sa.String(), nullable=True),
                    sa.PrimaryKeyConstraint('id')
                    )
    op.create_index(op.f('ix_ketl_api_config_description'), 'ketl_api_config', ['description'], unique=False)
    op.create_index(op.f('ix_ketl_api_config_name'), 'ketl_api_config', ['name'], unique=True)
    op.create_table('ketl_creds',
                    sa.Column('id', sa.Integer(), nullable=False),
                    sa.Column('api_config_id', sa.Integer(), nullable=True),
                    sa.Column('creds_details', JSON_COL(**col_args)),
                    sa.ForeignKeyConstraint(['api_config_id'], ['ketl_api_config.id'], ondelete='CASCADE'),
                    sa.PrimaryKeyConstraint('id')
                    )
    op.create_table('ketl_source',
                    sa.Column('id', sa.Integer(), nullable=False),
                    sa.Column('source_type', sa.String(), nullable=True),
                    sa.Column('base_url', sa.String(), nullable=True),
                    sa.Column('data_dir', sa.String(), nullable=True),
                    sa.Column('api_config_id', sa.Integer(), nullable=True),
                    sa.Column('meta', JSON_COL(**col_args)),
                    sa.ForeignKeyConstraint(['api_config_id'], ['ketl_api_config.id'], ondelete='CASCADE'),
                    sa.PrimaryKeyConstraint('id'),
                    sa.UniqueConstraint('base_url', 'data_dir', 'api_config_id')
                    )
    op.create_index(op.f('ix_ketl_source_base_url'), 'ketl_source', ['base_url'], unique=False)
    op.create_index(op.f('ix_ketl_source_data_dir'), 'ketl_source', ['data_dir'], unique=False)
    # if use_postgres:
    op.create_index('ix_ketl_source_meta', 'ketl_source', ['meta'], unique=False, **index_args)
    op.create_index(op.f('ix_ketl_source_source_type'), 'ketl_source', ['source_type'], unique=False)
    op.create_table('ketl_cached_file',
                    sa.Column('id', sa.Integer(), nullable=False),
                    sa.Column('source_id', sa.Integer(), nullable=True),
                    sa.Column('url', sa.String(), nullable=True),
                    sa.Column('url_params', JSON_COL(**col_args)),
                    sa.Column('path', sa.String(), nullable=True),
                    sa.Column('last_download', sa.DateTime(), nullable=True),
                    sa.Column('last_update', sa.DateTime(), nullable=True),
                    sa.Column('refresh_interval', sa.Interval(), nullable=True),
                    sa.Column('hash', sa.String(), nullable=True),
                    sa.Column('cache_type', sa.String(), nullable=True),
                    sa.Column('size', sa.BigInteger(), nullable=True),
                    sa.Column('is_archive', sa.Boolean(), nullable=True),
                    sa.Column('extract_to', sa.String(), nullable=True),
                    sa.Column('expected_mode', sa.Enum('auto', 'explicit', 'self', name='expectedmode'), nullable=True),
                    sa.Column('meta', JSON_COL(**col_args)),
                    sa.ForeignKeyConstraint(['source_id'], ['ketl_source.id'], ondelete='CASCADE'),
                    sa.PrimaryKeyConstraint('id'),
                    sa.UniqueConstraint('source_id', 'url', 'path')
                    )
    op.create_index(op.f('ix_ketl_cached_file_cache_type'), 'ketl_cached_file', ['cache_type'], unique=False)
    op.create_index(op.f('ix_ketl_cached_file_expected_mode'), 'ketl_cached_file', ['expected_mode'], unique=False)
    op.create_index(op.f('ix_ketl_cached_file_extract_to'), 'ketl_cached_file', ['extract_to'], unique=False)
    op.create_index(op.f('ix_ketl_cached_file_is_archive'), 'ketl_cached_file', ['is_archive'], unique=False)
    op.create_index(op.f('ix_ketl_cached_file_last_download'), 'ketl_cached_file', ['last_download'], unique=False)
    op.create_index(op.f('ix_ketl_cached_file_last_update'), 'ketl_cached_file', ['last_update'], unique=False)
    op.create_index('ix_ketl_cached_file_meta', 'ketl_cached_file', ['meta'], unique=False, **index_args)
    op.create_index(op.f('ix_ketl_cached_file_path'), 'ketl_cached_file', ['path'], unique=False)
    op.create_index(op.f('ix_ketl_cached_file_refresh_interval'), 'ketl_cached_file', ['refresh_interval'],
                    unique=False)
    op.create_index(op.f('ix_ketl_cached_file_size'), 'ketl_cached_file', ['size'], unique=False)
    op.create_index(op.f('ix_ketl_cached_file_url'), 'ketl_cached_file', ['url'], unique=False)
    op.create_table('ketl_expected_file',
                    sa.Column('id', sa.Integer(), nullable=False),
                    sa.Column('path', sa.String(), nullable=True),
                    sa.Column('archive_path', sa.String(), nullable=True),
                    sa.Column('hash', sa.String(), nullable=True),
                    sa.Column('size', sa.BigInteger(), nullable=True),
                    sa.Column('cached_file_id', sa.Integer(), nullable=True),
                    sa.Column('processed', sa.Boolean(), nullable=True),
                    sa.Column('file_type', sa.String(), nullable=True),
                    sa.Column('last_processed', sa.DateTime(), nullable=True),
                    sa.Column('meta', JSON_COL(**col_args)),
                    sa.ForeignKeyConstraint(['cached_file_id'], ['ketl_cached_file.id'], ondelete='CASCADE'),
                    sa.PrimaryKeyConstraint('id'),
                    sa.UniqueConstraint('path', 'cached_file_id')
                    )
    op.create_index(op.f('ix_ketl_expected_file_archive_path'), 'ketl_expected_file', ['archive_path'], unique=False)
    op.create_index(op.f('ix_ketl_expected_file_file_type'), 'ketl_expected_file', ['file_type'], unique=False)
    op.create_index(op.f('ix_ketl_expected_file_last_processed'), 'ketl_expected_file', ['last_processed'],
                    unique=False)
    op.create_index('ix_ketl_expected_file_meta', 'ketl_expected_file', ['meta'], unique=False, **index_args)
    op.create_index(op.f('ix_ketl_expected_file_path'), 'ketl_expected_file', ['path'], unique=False)
    op.create_index(op.f('ix_ketl_expected_file_processed'), 'ketl_expected_file', ['processed'], unique=False)
    op.create_index(op.f('ix_ketl_expected_file_size'), 'ketl_expected_file', ['size'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_ketl_expected_file_size'), table_name='ketl_expected_file')
    op.drop_index(op.f('ix_ketl_expected_file_processed'), table_name='ketl_expected_file')
    op.drop_index(op.f('ix_ketl_expected_file_path'), table_name='ketl_expected_file')
    op.drop_index('ix_ketl_expected_file_meta', table_name='ketl_expected_file')
    op.drop_index(op.f('ix_ketl_expected_file_last_processed'), table_name='ketl_expected_file')
    op.drop_index(op.f('ix_ketl_expected_file_file_type'), table_name='ketl_expected_file')
    op.drop_index(op.f('ix_ketl_expected_file_archive_path'), table_name='ketl_expected_file')
    op.drop_table('ketl_expected_file')
    op.drop_index(op.f('ix_ketl_cached_file_url'), table_name='ketl_cached_file')
    op.drop_index(op.f('ix_ketl_cached_file_size'), table_name='ketl_cached_file')
    op.drop_index(op.f('ix_ketl_cached_file_refresh_interval'), table_name='ketl_cached_file')
    op.drop_index(op.f('ix_ketl_cached_file_path'), table_name='ketl_cached_file')
    op.drop_index('ix_ketl_cached_file_meta', table_name='ketl_cached_file')
    op.drop_index(op.f('ix_ketl_cached_file_last_update'), table_name='ketl_cached_file')
    op.drop_index(op.f('ix_ketl_cached_file_last_download'), table_name='ketl_cached_file')
    op.drop_index(op.f('ix_ketl_cached_file_is_archive'), table_name='ketl_cached_file')
    op.drop_index(op.f('ix_ketl_cached_file_extract_to'), table_name='ketl_cached_file')
    op.drop_index(op.f('ix_ketl_cached_file_expected_mode'), table_name='ketl_cached_file')
    op.drop_index(op.f('ix_ketl_cached_file_cache_type'), table_name='ketl_cached_file')
    op.drop_table('ketl_cached_file')
    op.drop_index(op.f('ix_ketl_source_source_type'), table_name='ketl_source')
    op.drop_index('ix_ketl_source_meta', table_name='ketl_source')
    op.drop_index(op.f('ix_ketl_source_data_dir'), table_name='ketl_source')
    op.drop_index(op.f('ix_ketl_source_base_url'), table_name='ketl_source')
    op.drop_table('ketl_source')
    op.drop_table('ketl_creds')
    op.drop_index(op.f('ix_ketl_api_config_name'), table_name='ketl_api_config')
    op.drop_index(op.f('ix_ketl_api_config_description'), table_name='ketl_api_config')
    op.drop_table('ketl_api_config')
    # ### end Alembic commands ###
