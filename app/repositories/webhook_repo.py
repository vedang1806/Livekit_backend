"""
app/repositories/webhook_repo.py — Append-only audit log for LiveKit webhook events.
"""

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import WebhookEvent


class WebhookRepository:

    def __init__(self, db: AsyncSession) -> None:
        self._db = db

    async def log(self, event_type: str, room_name: str, payload: dict) -> WebhookEvent:
        event = WebhookEvent(
            event_type=event_type,
            room_name=room_name,
            payload=payload,
        )
        self._db.add(event)
        await self._db.flush()
        return event
