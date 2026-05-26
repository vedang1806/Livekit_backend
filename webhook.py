"""
webhook.py — LiveKit webhook handler.

Key design decisions:
- Composite egress auto-terminates when room empties (departureTimeout) → EGRESS_COMPLETE
- Never manually stop composite — it causes EGRESS_ABORTED
- Track egresses end naturally when participants leave
- SSE notification fires on composite egress_ended
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging

import jwt as pyjwt
from fastapi import HTTPException, Request

from config import settings
from egress import list_egress, start_composite_egress, start_track_egress

logger = logging.getLogger(__name__)

# ── In-memory state ──────────────────────────────────────────────────────────
# NOTE: All state is lost on server restart. For production, use Redis.

# room_name → composite_egress_id
# Kept alive until egress_ended fires so SSE notification works correctly.
_active_egress: dict[str, str] = {}

# (room_name, track_sid) — prevents duplicate per-track egress
_active_track_egress: set[tuple] = set()

# track_egress_id → room_name
_track_egress_id_to_room: dict[str, str] = {}

# Dedup: egress IDs already processed
_ended_egress_ids: set[str] = set()

# SSE: room_name → [asyncio.Queue]
_sse_subscribers: dict[str, list] = {}

# Rooms fully destroyed — guards against late-arriving track_published webhooks
_finished_rooms: set[str] = set()

# ── LiveKit track type/source constants (webhook sends strings) ───────────────
_AUDIO_TYPES = {"AUDIO"}
_AUDIO_SOURCES = {"MICROPHONE", "SCREEN_SHARE_AUDIO"}


def verify_livekit_signature(body: bytes, auth_header: str) -> bool:
    """Verify LiveKit JWT webhook signature."""
    try:
        claims = pyjwt.decode(
            auth_header.strip(),
            settings.livekit_api_secret,
            algorithms=["HS256"],
            leeway=30,
        )
        body_hash = base64.b64encode(hashlib.sha256(body).digest()).decode()
        return hmac.compare_digest(claims.get("sha256", ""), body_hash)
    except Exception as e:
        logger.error(f"Webhook signature verification failed: {e}")
        return False


async def handle_livekit_webhook(request: Request) -> dict:
    """Verify, parse, and dispatch webhook events. Always returns 200 immediately."""
    body = await request.body()
    auth_header = request.headers.get("Authorization", "")

    logger.info(f"Webhook Authorization header: {auth_header[:80]}...")
    logger.info(f"Webhook body (first 200): {body[:200]}")

    if not verify_livekit_signature(body, auth_header):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")

    try:
        event = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    event_type = event.get("event", "")
    room = event.get("room", {})
    room_name = (
        room.get("name")
        or event.get("egressInfo", {}).get("roomName", "")
    )

    logger.info(f"LiveKit webhook: {event_type} | room={room_name}")

    dispatch = {
        "track_published": lambda: asyncio.create_task(
            _on_track_published(room_name, event)
        ),
        "egress_ended": lambda: asyncio.create_task(
            _on_egress_ended(room_name, event)
        ),
        "room_finished": lambda: asyncio.create_task(
            _on_room_finished(room_name)
        ),
    }

    handler = dispatch.get(event_type)
    if handler:
        handler()
    else:
        logger.debug(f"⊘ Ignoring event: {event_type}")

    return {"received": True}


async def _on_track_published(room_name: str, event: dict) -> None:
    """
    Start composite (once per room) and per-participant OGG on every audio track.
    Fires after track is live — no delay needed.
    """
    try:
        if not room_name:
            logger.warning("⚠️  _on_track_published: missing room_name")
            return

        if room_name in _finished_rooms:
            logger.warning(f"⚠️  Late track_published for finished room {room_name} — ignored")
            return

        track = event.get("track", {})
        track_sid = track.get("sid", "")
        track_type = str(track.get("type", "")).upper()
        track_source = str(track.get("source", "")).upper()
        participant = event.get("participant", {})
        identity = participant.get("identity", "unknown")

        logger.info(
            f"📊 Track published: type={track_type} source={track_source} "
            f"sid={track_sid} participant={identity}"
        )

        # ✅ Correct audio detection — strings only, no ints
        is_audio = track_type in _AUDIO_TYPES or track_source in _AUDIO_SOURCES
        if not is_audio:
            logger.info(f"⊘ Non-audio track skipped: type={track_type} source={track_source}")
            return

        logger.info("✅ Audio track confirmed — proceeding with egress")

        # ── 1. Composite egress (once per room) ──────────────────────────────
        if room_name not in _active_egress:
            await _ensure_composite_egress(room_name)

        # ── 2. Per-participant OGG track egress ───────────────────────────────
        await _ensure_track_egress(room_name, track_sid, identity)

    except Exception as e:
        logger.error(f"❌ _on_track_published error: {e}", exc_info=True)


async def _ensure_composite_egress(room_name: str) -> None:
    """Start composite egress for a room if not already running."""
    try:
        existing = await list_egress(room_name=room_name)
        running = [
            e for e in existing.get("items", [])
            if e.get("status") in ("EGRESS_STARTING", "EGRESS_ACTIVE")
            # Only match composite egress (not track egress)
            and e.get("sourceType") != "EGRESS_SOURCE_TYPE_SDK"
        ]
        if running:
            _active_egress[room_name] = running[0].get("egressId", "")
            logger.info(f"✓ Composite already running for {room_name}: {_active_egress[room_name]}")
            return

        result = await start_composite_egress(
            room_name=room_name,
            session_id=room_name,
            audio_only=False,
        )
        egress_id = result.get("egressId") or result.get("egress_id", "")
        _active_egress[room_name] = egress_id
        logger.info(f"✅ Composite started: {egress_id} | {result.get('s3_url', '')}")

    except Exception as e:
        logger.error(f"❌ Composite egress FAILED for {room_name}: {e}", exc_info=True)


async def _ensure_track_egress(room_name: str, track_sid: str, identity: str) -> None:
    """Start OGG track egress for a participant if not already running."""
    key = (room_name, track_sid)
    if key in _active_track_egress:
        logger.info(f"⊘ OGG already active for track {track_sid}")
        return

    try:
        result = await start_track_egress(
            room_name=room_name,
            session_id=room_name,
            track_sid=track_sid,
            identity=identity,
        )
        _active_track_egress.add(key)
        track_egress_id = result.get("egressId") or result.get("egress_id", "")
        if track_egress_id:
            _track_egress_id_to_room[track_egress_id] = room_name
        logger.info(
            f"✅ OGG started: {identity} ({track_sid}) → "
            f"s3://{settings.s3_bucket}/{result.get('s3_key', '')}"
        )
    except Exception as e:
        logger.error(f"❌ Track egress FAILED for {identity} ({track_sid}): {e}", exc_info=True)


async def _on_egress_ended(room_name: str, event: dict) -> None:
    """
    Handle egress_ended for both composite and track egresses.

    Composite egress lifecycle:
      All participants leave → departureTimeout (20s) → room_finished
      → LiveKit auto-terminates composite → egress_ended(EGRESS_COMPLETE)

    We never manually stop composite — that causes EGRESS_ABORTED.
    """
    egress_info = event.get("egressInfo", {})
    egress_id = egress_info.get("egressId", "")
    status = egress_info.get("status", "")

    if egress_id in _ended_egress_ids:
        logger.info(f"⊘ Duplicate egress_ended ignored: {egress_id}")
        return
    _ended_egress_ids.add(egress_id)

    is_composite = (_active_egress.get(room_name) == egress_id)
    is_track = (egress_id in _track_egress_id_to_room)

    logger.info(
        f"Egress ended: {egress_id} | room={room_name} | status={status} "
        f"| composite={is_composite} | track={is_track}"
    )

    if is_composite:
        # ✅ Clean up composite state AFTER confirming egress_ended
        _active_egress.pop(room_name, None)

        if status == "EGRESS_COMPLETE":
            logger.info(f"🎬 Composite recording finalized for {room_name}")
        else:
            # EGRESS_ABORTED means MP4 may be corrupt — log clearly
            logger.error(
                f"🚨 Composite egress ABORTED for {room_name} — "
                f"MP4 may be incomplete. Status: {status}"
            )

        # Notify SSE subscribers — recording is saved (or failed)
        for queue in _sse_subscribers.get(room_name, []):
            await queue.put({
                "event": "egress_ended",
                "session_id": room_name,
                "status": status,
                "success": status == "EGRESS_COMPLETE",
            })

    if is_track:
        _track_egress_id_to_room.pop(egress_id, None)
        remaining_count = sum(
            1 for rn in _track_egress_id_to_room.values() if rn == room_name
        )
        logger.info(
            f"🎙️  Track egress done: {egress_id} | "
            f"remaining for {room_name}: {remaining_count}"
        )
        # ✅ Do NOT stop composite here.
        # Participants leaving ≠ session over.
        # Composite auto-terminates when room_finished fires.


async def _on_room_finished(room_name: str) -> None:
    """
    Room is fully destroyed (all participants left + departureTimeout expired).

    At this point LiveKit has already auto-terminated the composite egress.
    We will receive egress_ended(EGRESS_COMPLETE) shortly after.

    All we do here is mark the room finished and clean up track state.
    We intentionally keep _active_egress[room_name] intact so that the
    incoming egress_ended can still match is_composite=True and fire SSE.
    """
    _finished_rooms.add(room_name)

    # Clean up track egress state (track egresses already ended via egress_ended)
    stale_track_keys = {k for k in _active_track_egress if k[0] == room_name}
    _active_track_egress.difference_update(stale_track_keys)

    stale_egress_ids = {
        eid for eid, rn in _track_egress_id_to_room.items() if rn == room_name
    }
    for eid in stale_egress_ids:
        _track_egress_id_to_room.pop(eid, None)

    logger.info(
        f"🚪 Room finished: {room_name} — composite will auto-finalize, "
        f"waiting for egress_ended"
    )
    # NOTE: _active_egress[room_name] intentionally NOT cleared here.
    # It must survive until egress_ended fires so is_composite check works.


# ── SSE helpers ───────────────────────────────────────────────────────────────

def subscribe_sse(room_name: str) -> asyncio.Queue:
    q = asyncio.Queue()
    _sse_subscribers.setdefault(room_name, []).append(q)
    return q


def unsubscribe_sse(room_name: str, queue: asyncio.Queue) -> None:
    subs = _sse_subscribers.get(room_name, [])
    if queue in subs:
        subs.remove(queue)


def clear_active_egress(room_name: str) -> None:
    """
    Called from manual /egress/stop endpoint.
    Only use this if you have a 'Stop Recording' button in your UI.
    """
    _active_egress.pop(room_name, None)
    stale_tracks = {k for k in _active_track_egress if k[0] == room_name}
    _active_track_egress.difference_update(stale_tracks)
    stale_ids = {eid for eid, rn in _track_egress_id_to_room.items() if rn == room_name}
    for eid in stale_ids:
        _track_egress_id_to_room.pop(eid, None)
    _finished_rooms.discard(room_name)
