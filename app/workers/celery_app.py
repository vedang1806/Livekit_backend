"""
app/workers/celery_app.py — Celery application instance.

Redis URL is built from individual REDIS_* env vars via settings.redis_url.
"""

from app.config import settings
from celery import Celery

celery_app = Celery(
    "livekit_workers",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=["app.workers.tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_default_retry_delay=60,
    task_max_retries=3,
    broker_connection_retry_on_startup=True,
)
