"""
app/state/session.py — In-process session state for the LiveKit interpreter service.

Single source of truth for everything that changes during a session lifecycle:
participants, egress IDs, SSE queues, S3 URLs.

Design notes:
- All state is encapsulated in SessionState. Nothing leaks into module globals.
- Methods are synchronous (no I/O). Callers (webhook handlers) stay clean.
- TTL-bounded dicts prevent unbounded memory growth on long-running servers.
- The periodic GC coroutine lives here — start it once from app lifespan.
- Swapping for Redis later means only rewriting this file.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

_FINISHED_ROOM_TTL = 3600    # 1 hour
_ENDED_EGRESS_TTL  = 86400   # 24 hours


@dataclass
class S3Urls:
    composite: str = ""
    audio: dict[str, str] = field(default_factory=dict)   # identity → s3_url
    video: dict[str, str] = field(default_factory=dict)   # identity → s3_url


class SessionState:
    """
    All mutable state for live sessions.
    Import the singleton: `from app.state.session import state`
    """

    def __init__(self) -> None:
        self._active_egress: dict[str, str] = {}
        self._composite_ready: set[str] = set()
        self._pending_stop: set[str] = set()
        self._active_track_egress: set[tuple] = set()
        self._track_egress_id_to_room: dict[str, str] = {}
        self._active_participants: dict[str, set[str]] = {}
        self._finished_rooms: dict[str, float] = {}
        self._ended_egress_ids: dict[str, float] = {}
        self._sse_subscribers: dict[str, list[asyncio.Queue]] = {}
        self._s3_urls: dict[str, S3Urls] = {}

    # ── Participants ──────────────────────────────────────────────────────────

    def add_participant(self, room_name: str, identity: str) -> int:
        self._active_participants.setdefault(room_name, set()).add(identity)
        return len(self._active_participants[room_name])

    def remove_participant(self, room_name: str, identity: str) -> int:
        self._active_participants.get(room_name, set()).discard(identity)
        return len(self._active_participants.get(room_name, set()))

    def participant_count(self, room_name: str) -> int:
        return len(self._active_participants.get(room_name, set()))

    # ── Composite egress ──────────────────────────────────────────────────────

    def set_composite_egress(self, room_name: str, egress_id: str) -> None:
        self._active_egress[room_name] = egress_id

    def get_composite_egress(self, room_name: str) -> Optional[str]:
        return self._active_egress.get(room_name)

    def clear_composite_egress(self, room_name: str) -> None:
        self._active_egress.pop(room_name, None)

    def has_composite_egress(self, room_name: str) -> bool:
        return room_name in self._active_egress

    # ── Composite ready / pending-stop flags ──────────────────────────────────

    def mark_composite_ready(self, room_name: str) -> None:
        self._composite_ready.add(room_name)

    def is_composite_ready(self, room_name: str) -> bool:
        return room_name in self._composite_ready

    def clear_composite_ready(self, room_name: str) -> None:
        self._composite_ready.discard(room_name)

    def mark_pending_stop(self, room_name: str) -> None:
        self._pending_stop.add(room_name)

    def is_pending_stop(self, room_name: str) -> bool:
        return room_name in self._pending_stop

    def clear_pending_stop(self, room_name: str) -> None:
        self._pending_stop.discard(room_name)

    # ── Track egress ──────────────────────────────────────────────────────────

    def has_track_egress(self, room_name: str, track_sid: str) -> bool:
        return (room_name, track_sid) in self._active_track_egress

    def add_track_egress(self, room_name: str, track_sid: str, egress_id: str) -> None:
        self._active_track_egress.add((room_name, track_sid))
        if egress_id:
            self._track_egress_id_to_room[egress_id] = room_name

    def is_track_egress_id(self, egress_id: str) -> bool:
        return egress_id in self._track_egress_id_to_room

    def get_room_for_track_egress(self, egress_id: str) -> Optional[str]:
        return self._track_egress_id_to_room.get(egress_id)

    def remove_track_egress_id(self, egress_id: str) -> None:
        self._track_egress_id_to_room.pop(egress_id, None)

    def remaining_track_egress_count(self, room_name: str) -> int:
        return sum(1 for rn in self._track_egress_id_to_room.values() if rn == room_name)

    # ── Finished rooms (TTL-bounded) ──────────────────────────────────────────

    def mark_room_finished(self, room_name: str) -> None:
        self._finished_rooms[room_name] = time.monotonic()

    def is_room_finished(self, room_name: str) -> bool:
        return room_name in self._finished_rooms

    def unmark_room_finished(self, room_name: str) -> None:
        self._finished_rooms.pop(room_name, None)

    # ── Egress dedup (TTL-bounded) ────────────────────────────────────────────

    def seen_egress_ended(self, egress_id: str) -> bool:
        return egress_id in self._ended_egress_ids

    def mark_egress_ended(self, egress_id: str) -> None:
        self._ended_egress_ids[egress_id] = time.monotonic()

    # ── S3 URL tracking ───────────────────────────────────────────────────────

    def set_composite_url(self, room_name: str, url: str) -> None:
        self._s3_urls.setdefault(room_name, S3Urls()).composite = url

    def set_track_url(self, room_name: str, kind: str, identity: str, url: str) -> None:
        urls = self._s3_urls.setdefault(room_name, S3Urls())
        (urls.audio if kind == "audio" else urls.video)[identity] = url

    def get_s3_urls(self, room_name: str) -> Optional[S3Urls]:
        return self._s3_urls.get(room_name)

    # ── SSE subscriptions ─────────────────────────────────────────────────────

    def subscribe_sse(self, room_name: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self._sse_subscribers.setdefault(room_name, []).append(q)
        return q

    def unsubscribe_sse(self, room_name: str, queue: asyncio.Queue) -> None:
        subs = self._sse_subscribers.get(room_name, [])
        try:
            subs.remove(queue)
        except ValueError:
            pass
        if not subs:
            self._sse_subscribers.pop(room_name, None)

    def get_sse_subscribers(self, room_name: str) -> list[asyncio.Queue]:
        return self._sse_subscribers.get(room_name, [])

    # ── Full room cleanup ─────────────────────────────────────────────────────

    def cleanup_room(self, room_name: str) -> None:
        self._active_participants.pop(room_name, None)
        self._active_egress.pop(room_name, None)
        self._composite_ready.discard(room_name)
        self._pending_stop.discard(room_name)
        self._s3_urls.pop(room_name, None)
        stale_tracks = {k for k in self._active_track_egress if k[0] == room_name}
        self._active_track_egress.difference_update(stale_tracks)
        stale_ids = [eid for eid, rn in self._track_egress_id_to_room.items() if rn == room_name]
        for eid in stale_ids:
            self._track_egress_id_to_room.pop(eid, None)

    # ── Periodic GC ───────────────────────────────────────────────────────────

    async def run_gc(self) -> None:
        """Evict expired TTL entries every 5 minutes. Start from app lifespan."""
        while True:
            await asyncio.sleep(300)
            now = time.monotonic()
            stale_rooms = [k for k, t in self._finished_rooms.items() if now - t > _FINISHED_ROOM_TTL]
            for k in stale_rooms:
                self._finished_rooms.pop(k, None)
            stale_egress = [k for k, t in self._ended_egress_ids.items() if now - t > _ENDED_EGRESS_TTL]
            for k in stale_egress:
                self._ended_egress_ids.pop(k, None)
            if stale_rooms or stale_egress:
                logger.debug(f"GC: evicted {len(stale_rooms)} rooms, {len(stale_egress)} egress IDs")

    # ── Debug snapshot ────────────────────────────────────────────────────────

    def snapshot(self) -> dict:
        """JSON-serialisable summary — expose via a /debug/state admin endpoint."""
        return {
            "active_egress":        dict(self._active_egress),
            "composite_ready":      list(self._composite_ready),
            "pending_stop":         list(self._pending_stop),
            "active_participants":  {k: list(v) for k, v in self._active_participants.items()},
            "active_track_egress":  [list(t) for t in self._active_track_egress],
            "track_egress_count":   len(self._track_egress_id_to_room),
            "finished_rooms":       list(self._finished_rooms.keys()),
            "ended_egress_count":   len(self._ended_egress_ids),
            "sse_subscriber_rooms": list(self._sse_subscribers.keys()),
        }


# ── Singleton ─────────────────────────────────────────────────────────────────

state = SessionState()
