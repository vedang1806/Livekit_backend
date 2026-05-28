"""
app/repositories/session_repo.py — CRUD for Session and Participant models.
"""

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Participant, ParticipantRole, Session, SessionStatus


class SessionRepository:

    def __init__(self, db: AsyncSession) -> None:
        self._db = db

    async def get_or_create(self, room_name: str) -> Session:
        """Return the active session for room_name, creating one if it doesn't exist."""
        result = await self._db.execute(
            select(Session).where(Session.room_name == room_name, Session.status == SessionStatus.active)
        )
        session = result.scalar_one_or_none()
        if session is None:
            session = Session(room_name=room_name, status=SessionStatus.active)
            self._db.add(session)
            await self._db.flush()  # get auto-assigned id without committing
        return session

    async def get_active(self, room_name: str) -> Session | None:
        result = await self._db.execute(
            select(Session).where(Session.room_name == room_name, Session.status == SessionStatus.active)
        )
        return result.scalar_one_or_none()

    async def end_session(self, room_name: str) -> Session | None:
        session = await self.get_active(room_name)
        if session:
            session.status   = SessionStatus.ended
            session.ended_at = datetime.now(timezone.utc)
            await self._db.flush()
        return session

    async def add_participant(
        self,
        session_id: int,
        identity: str,
        role: ParticipantRole = ParticipantRole.unknown,
    ) -> Participant:
        participant = Participant(session_id=session_id, identity=identity, role=role)
        self._db.add(participant)
        await self._db.flush()
        return participant

    async def get_active_participant(self, session_id: int, identity: str) -> Participant | None:
        result = await self._db.execute(
            select(Participant).where(
                Participant.session_id == session_id,
                Participant.identity   == identity,
                Participant.left_at.is_(None),
            )
        )
        return result.scalar_one_or_none()

    async def mark_participant_left(self, session_id: int, identity: str) -> Participant | None:
        participant = await self.get_active_participant(session_id, identity)
        if participant:
            participant.left_at = datetime.now(timezone.utc)
            await self._db.flush()
        return participant

    async def set_composite_s3_url(self, room_name: str, s3_url: str) -> None:
        session = await self.get_active(room_name)
        if session:
            session.composite_s3_url = s3_url
            await self._db.flush()

    async def set_participant_track_s3_url(self, session_id: int, identity: str, s3_url: str) -> None:
        participant = await self.get_active_participant(session_id, identity)
        if participant:
            participant.track_s3_url = s3_url
            await self._db.flush()
