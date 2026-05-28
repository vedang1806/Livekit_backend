"""
app/workers/tasks.py — Celery tasks for HIPAA compliance video analysis.

Analyzes the interpreter's per-track WebM file only.
Expected face count is always 1 — only the interpreter should be visible.
"""

import logging
from datetime import datetime, timezone

from celery import Task
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session as SyncSession

from app.config import settings
from app.db.models import ComplianceReport, ComplianceStatus
from app.services.rekognition import analyze_recording
from app.workers.celery_app import celery_app

logger = logging.getLogger(__name__)

_sync_engine = create_engine(
    settings.database_url_sync,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
)


def _get_sync_db() -> SyncSession:
    return SyncSession(_sync_engine)


class BaseTask(Task):
    abstract = True

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        egress_job_id = kwargs.get("egress_job_id") or (args[0] if args else None)
        if egress_job_id:
            _mark_error(egress_job_id, str(exc))
        logger.error(f"Task {self.name}[{task_id}] failed: {exc}", exc_info=True)


def _mark_error(egress_job_id: int, detail: str) -> None:
    with _get_sync_db() as db:
        report = db.execute(
            select(ComplianceReport).where(ComplianceReport.egress_job_id == egress_job_id)
        ).scalar_one_or_none()
        if report:
            report.status       = ComplianceStatus.error
            report.error_detail = detail[:2000]
            report.processed_at = datetime.now(timezone.utc)
            db.commit()


@celery_app.task(
    bind=True,
    base=BaseTask,
    name="compliance.run_check",
    max_retries=3,
    default_retry_delay=60,
)
def run_compliance_check(
    self,
    egress_job_id: int,
    s3_key: str,
    participant_identity: str,
    expected_participant_count: int = 1,  # always 1 for interpreter track
) -> dict:
    """
    Analyze the interpreter's per-track WebM for HIPAA compliance.
    Flags any frame where more than 1 face is detected.
    """
    logger.info(
        f"Compliance check starting | egress_job_id={egress_job_id} "
        f"identity={participant_identity} s3_key={s3_key}"
    )

    with _get_sync_db() as db:
        report = db.execute(
            select(ComplianceReport).where(ComplianceReport.egress_job_id == egress_job_id)
        ).scalar_one_or_none()

        if not report:
            logger.error(f"ComplianceReport not found for egress_job_id={egress_job_id}")
            return {"error": "report not found"}

        report.status = ComplianceStatus.processing
        db.commit()

    try:
        result = analyze_recording(s3_key, expected_participant_count)
    except Exception as exc:
        logger.error(f"analyze_recording raised: {exc}", exc_info=True)
        raise self.retry(exc=exc)

    with _get_sync_db() as db:
        report = db.execute(
            select(ComplianceReport).where(ComplianceReport.egress_job_id == egress_job_id)
        ).scalar_one()

        if result.error and result.frames_analyzed == 0:
            report.status       = ComplianceStatus.error
            report.error_detail = result.error
        else:
            report.status             = ComplianceStatus.passed if result.passed else ComplianceStatus.failed
            report.max_faces_detected = result.max_faces
            report.frames_analyzed    = result.frames_analyzed
            report.violation_frames   = result.violation_summary()
            if result.error:
                report.error_detail = result.error

        report.processed_at = datetime.now(timezone.utc)
        db.commit()

    outcome = "PASSED" if result.passed else "FAILED — HIPAA VIOLATION"
    logger.info(
        f"Compliance check {outcome} | identity={participant_identity} "
        f"egress_job_id={egress_job_id} violations={len(result.violations)} "
        f"max_faces={result.max_faces} frames={result.frames_analyzed}"
    )

    return {
        "egress_job_id":       egress_job_id,
        "participant_identity": participant_identity,
        "passed":              result.passed,
        "frames_analyzed":     result.frames_analyzed,
        "max_faces":           result.max_faces,
        "violation_count":     len(result.violations),
    }
