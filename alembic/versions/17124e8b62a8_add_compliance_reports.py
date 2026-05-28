"""add_compliance_reports

Revision ID: 17124e8b62a8
Revises: 1b23e52a6e53
Create Date: 2026-05-28
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "17124e8b62a8"
down_revision: Union[str, Sequence[str], None] = "1b23e52a6e53"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "compliance_reports",
        sa.Column("id",                  sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("egress_job_id",       sa.BigInteger(), nullable=False),
        sa.Column("status",              sa.Enum("pending", "processing", "passed", "failed", "error", name="compliancestatus"), nullable=False),
        sa.Column("expected_face_count", sa.Integer(), nullable=False),
        sa.Column("max_faces_detected",  sa.Integer(), nullable=True),
        sa.Column("violation_frames",    postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("frames_analyzed",     sa.Integer(), nullable=True),
        sa.Column("created_at",          sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("processed_at",        sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_detail",        sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["egress_job_id"], ["egress_jobs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("egress_job_id"),
    )
    op.create_index("ix_compliance_reports_egress_job_id", "compliance_reports", ["egress_job_id"])


def downgrade() -> None:
    op.drop_table("compliance_reports")
    op.execute("DROP TYPE IF EXISTS compliancestatus")
