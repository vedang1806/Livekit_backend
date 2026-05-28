"""add_s3_urls_to_sessions_and_participants

Revision ID: d1b674c334d3
Revises: c8939b0d02a4
Create Date: 2026-05-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "d1b674c334d3"
down_revision: Union[str, Sequence[str], None] = "c8939b0d02a4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("sessions",      sa.Column("composite_s3_url", sa.Text(), nullable=True))
    op.add_column("participants",  sa.Column("track_s3_url",     sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("participants", "track_s3_url")
    op.drop_column("sessions",     "composite_s3_url")
