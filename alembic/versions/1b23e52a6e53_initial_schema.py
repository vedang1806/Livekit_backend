"""initial_schema

Revision ID: 1b23e52a6e53
Revises:
Create Date: 2026-05-28

Creates: sessions, participants, egress_jobs, recordings, webhook_events
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "1b23e52a6e53"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "sessions",
        sa.Column("id",         sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("room_name",  sa.String(255),  nullable=False),
        sa.Column("status",     sa.Enum("active", "ended", name="sessionstatus"), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("ended_at",   sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("room_name"),
    )
    op.create_index("ix_sessions_room_name", "sessions", ["room_name"])

    op.create_table(
        "participants",
        sa.Column("id",         sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("session_id", sa.BigInteger(), nullable=False),
        sa.Column("identity",   sa.String(255),  nullable=False),
        sa.Column("role",       sa.Enum("patient", "doctor", "interpreter", "unknown", name="participantrole"), nullable=False),
        sa.Column("joined_at",  sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("left_at",    sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["session_id"], ["sessions.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_participants_session_identity", "participants", ["session_id", "identity"])

    op.create_table(
        "egress_jobs",
        sa.Column("id",          sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("session_id",  sa.BigInteger(), nullable=False),
        sa.Column("egress_id",   sa.String(255),  nullable=False),
        sa.Column("egress_type", sa.Enum("composite", "track", name="egresstype"), nullable=False),
        sa.Column("track_sid",   sa.String(255),  nullable=True),
        sa.Column("identity",    sa.String(255),  nullable=True),
        sa.Column("status",      sa.Enum("starting", "active", "complete", "aborted", "failed", name="egressstatus"), nullable=False),
        sa.Column("started_at",  sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("ended_at",    sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["session_id"], ["sessions.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("egress_id"),
    )
    op.create_index("ix_egress_jobs_egress_id", "egress_jobs", ["egress_id"])

    op.create_table(
        "recordings",
        sa.Column("id",            sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("egress_job_id", sa.BigInteger(), nullable=False),
        sa.Column("s3_key",        sa.Text(),       nullable=False),
        sa.Column("file_type",     sa.String(10),   nullable=False),
        sa.Column("created_at",    sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["egress_job_id"], ["egress_jobs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "webhook_events",
        sa.Column("id",          sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("event_type",  sa.String(64),   nullable=False),
        sa.Column("room_name",   sa.String(255),  nullable=False),
        sa.Column("payload",     postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_webhook_events_event_type_room", "webhook_events", ["event_type", "room_name"])
    op.create_index("ix_webhook_events_received_at",     "webhook_events", ["received_at"])


def downgrade() -> None:
    op.drop_table("webhook_events")
    op.drop_table("recordings")
    op.drop_table("egress_jobs")
    op.drop_table("participants")
    op.drop_table("sessions")

    op.execute("DROP TYPE IF EXISTS sessionstatus")
    op.execute("DROP TYPE IF EXISTS participantrole")
    op.execute("DROP TYPE IF EXISTS egresstype")
    op.execute("DROP TYPE IF EXISTS egressstatus")
