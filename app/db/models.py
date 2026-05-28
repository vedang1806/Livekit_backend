"""
app/db/models.py — SQLAlchemy ORM models.

These are the persistent entities. Pydantic request/response schemas live in app/models.py.
"""

import enum
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ── Enums ─────────────────────────────────────────────────────────────────────

class SessionStatus(str, enum.Enum):
    active  = "active"
    ended   = "ended"


class EgressType(str, enum.Enum):
    composite = "composite"
    track     = "track"


class EgressStatus(str, enum.Enum):
    starting = "starting"
    active   = "active"
    complete = "complete"
    aborted  = "aborted"
    failed   = "failed"


class ParticipantRole(str, enum.Enum):
    patient     = "patient"
    doctor      = "doctor"
    interpreter = "interpreter"
    unknown     = "unknown"


# ── Models ────────────────────────────────────────────────────────────────────

class Session(Base):
    """One LiveKit room = one session."""
    __tablename__ = "sessions"

    id:         Mapped[int]      = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    room_name:  Mapped[str]      = mapped_column(String(255), nullable=False, unique=True, index=True)
    status:     Mapped[str]      = mapped_column(Enum(SessionStatus), nullable=False, default=SessionStatus.active)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    ended_at:   Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    participants: Mapped[list["Participant"]] = relationship(back_populates="session", cascade="all, delete-orphan")
    egress_jobs:  Mapped[list["EgressJob"]]  = relationship(back_populates="session", cascade="all, delete-orphan")


class Participant(Base):
    """Tracks every join/leave event for a room."""
    __tablename__ = "participants"
    __table_args__ = (
        Index("ix_participants_session_identity", "session_id", "identity"),
    )

    id:          Mapped[int]      = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    session_id:  Mapped[int]      = mapped_column(ForeignKey("sessions.id", ondelete="CASCADE"), nullable=False)
    identity:    Mapped[str]      = mapped_column(String(255), nullable=False)
    role:        Mapped[str]      = mapped_column(Enum(ParticipantRole), nullable=False, default=ParticipantRole.unknown)
    joined_at:   Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    left_at:     Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    session: Mapped["Session"] = relationship(back_populates="participants")


class EgressJob(Base):
    """
    One row per LiveKit egress (composite or per-track).
    egress_id is the LiveKit-assigned ID — used for dedup and status tracking.
    """
    __tablename__ = "egress_jobs"

    id:          Mapped[int]      = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    session_id:  Mapped[int]      = mapped_column(ForeignKey("sessions.id", ondelete="CASCADE"), nullable=False)
    egress_id:   Mapped[str]      = mapped_column(String(255), nullable=False, unique=True, index=True)
    egress_type: Mapped[str]      = mapped_column(Enum(EgressType), nullable=False)
    track_sid:   Mapped[str | None] = mapped_column(String(255), nullable=True)
    identity:    Mapped[str | None] = mapped_column(String(255), nullable=True)  # participant for track egress
    status:      Mapped[str]      = mapped_column(Enum(EgressStatus), nullable=False, default=EgressStatus.starting)
    started_at:  Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)
    ended_at:    Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    session:    Mapped["Session"]       = relationship(back_populates="egress_jobs")
    recordings: Mapped[list["Recording"]] = relationship(back_populates="egress_job", cascade="all, delete-orphan")


class Recording(Base):
    """S3 file produced by a completed egress job."""
    __tablename__ = "recordings"

    id:            Mapped[int]      = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    egress_job_id: Mapped[int]      = mapped_column(ForeignKey("egress_jobs.id", ondelete="CASCADE"), nullable=False)
    s3_key:        Mapped[str]      = mapped_column(Text, nullable=False)
    file_type:     Mapped[str]      = mapped_column(String(10), nullable=False)  # mp4 | ogg | webm
    created_at:    Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow)

    egress_job: Mapped["EgressJob"] = relationship(back_populates="recordings")


class WebhookEvent(Base):
    """
    Append-only audit log of every LiveKit webhook received.
    Used for idempotency checks and debugging.
    """
    __tablename__ = "webhook_events"
    __table_args__ = (
        Index("ix_webhook_events_event_type_room", "event_type", "room_name"),
    )

    id:          Mapped[int]      = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_type:  Mapped[str]      = mapped_column(String(64), nullable=False)
    room_name:   Mapped[str]      = mapped_column(String(255), nullable=False)
    payload:     Mapped[dict]     = mapped_column(JSONB, nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=_utcnow, index=True)
