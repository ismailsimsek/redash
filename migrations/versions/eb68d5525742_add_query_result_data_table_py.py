"""add_query_result_data_table.py

Revision ID: eb68d5525742
Revises: 73beceabb948
Create Date: 2019-01-22 13:22:23.204770

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'eb68d5525742'
down_revision = '73beceabb948'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('query_results', sa.Column('data_handler', sa.String(length=25), server_default='db', nullable=False))
    op.create_table('query_result_data',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('data', sa.Text(), nullable=True),
        # sa.ForeignKeyConstraint(['id'], ['query_results.id'], ), # TODO Do we need it ?
        sa.UniqueConstraint('id', name='uix_query_result_data__PK')
    )
    connection = op.get_bind()
    # 1. clean table
    connection.execute("DELETE FROM query_result_data")
    # 2. copy data to new table
    connection.execute("INSERT INTO query_result_data (id, data) SELECT id, data FROM query_results")
    # set data_handler for existing results
    connection.execute("UPDATE query_results SET data_handler='db'")


def downgrade():
    op.drop_column('query_results', 'data_handler')
    op.drop_table('query_result_data')
