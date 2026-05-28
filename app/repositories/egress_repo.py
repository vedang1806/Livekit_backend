"""
app/repositories/egress_repo.py — CRUD for EgressJob and Recording models.
"""

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.db.models import EgressJob, EgressStatus, EgressType, Recording


class EgressRepository:

    def __init__(self, db: AsyncSession) -> None:
        self._db = db

    async def create_egress(
        self,
        session_id: int,
        egress_id: str,
        egress_type: EgressType,
        track_sid: str | None = None,
        identity: str | None = None,
    ) -> EgressJob:
        job = EgressJob(
            session_id=session_id,
            egress_id=egress_id,
            egress_type=egress_type,
            track_sid=track_sid,
            identity=identity,
            status=EgressStatus.starting,
        )
        self._db.add(job)
        await self._db.flush()
        return job

    async def get_by_egress_id(self, egress_id: str) -> EgressJob | None:
        result = await self._db.execute(
            select(EgressJob).where(EgressJob.egress_id == egress_id)
        )
        return result.scalar_one_or_none()

    async def update_status(self, egress_id: str, status: EgressStatus) -> EgressJob | None:
        job = await self.get_by_egress_id(egress_id)
        if job:
            job.status = status
            if status in (EgressStatus.complete, EgressStatus.aborted, EgressStatus.failed):
                job.ended_at = datetime.now(timezone.utc)
            await self._db.flush()
        return job

    async def get_active_composite(self, session_id: int) -> EgressJob | None:
        result = await self._db.execute(
            select(EgressJob).where(
                EgressJob.session_id  == session_id,
                EgressJob.egress_type == EgressType.composite,
                EgressJob.status.in_([EgressStatus.starting, EgressStatus.active]),
            )
        )
        return result.scalar_one_or_none()

    async def list_for_session(self, session_id: int) -> list[EgressJob]:
        result = await self._db.execute(
            select(EgressJob)
            .where(EgressJob.session_id == session_id)
            .options(selectinload(EgressJob.recordings))
            .order_by(EgressJob.started_at)
        )
        return list(result.scalars().all())

    async def add_recording(
        self,
        egress_job_id: int,
        s3_key: str,
        file_type: str,
    ) -> Recording:
        recording = Recording(
            egress_job_id=egress_job_id,
            s3_key=s3_key,
            file_type=file_type,
        )
        self._db.add(recording)
        await self._db.flush()
        return recording
