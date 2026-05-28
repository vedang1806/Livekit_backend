"""compliance_report_add_s3_url_identity

Revision ID: c8939b0d02a4
Revises: 17124e8b62a8
Create Date: 2026-05-28

Adds s3_url and participant_identity to compliance_reports.
Drops the table and recreates — only safe because this migration runs
before any production data exists in compliance_reports.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "c8939b0d02a4"
down_revision: Union[str, Sequence[str], None] = "17124e8b62a8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("compliance_reports", sa.Column("participant_identity", sa.String(255), nullable=True))
    op.add_column("compliance_reports", sa.Column("s3_url", sa.Text(), nullable=True))
    # Backfill existing rows with placeholder so we can make them NOT NULL
    op.execute("UPDATE compliance_reports SET participant_identity = 'unknown', s3_url = '' WHERE participant_identity IS NULL")
    op.alter_column("compliance_reports", "participant_identity", nullable=False)
    op.alter_column("compliance_reports", "s3_url", nullable=False)


def downgrade() -> None:
    op.drop_column("compliance_reports", "s3_url")
    op.drop_column("compliance_reports", "participant_identity")
