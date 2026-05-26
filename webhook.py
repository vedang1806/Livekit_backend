"""
webhook.py — LiveKit webhook handler.

Egress lifecycle for custom layout composite:
  - Composite must be manually stopped to get EGRESS_COMPLETE
  - Stop window: after last participant_left, before room_finished
  - We listen to participant_left, count active participants,
    and stop composite when count reaches 0
  - departureTimeout=20s gives us the window
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
from egress import list_egress, start_composite_egress, start_track_egress, stop_egress

logger = logging.getLogger(__name__)

# ── In-memory state ──────────────────────────────────────────────────────────

_active_egress: dict[str, str] = {}           # room_name → composite_egress_id
_composite_ready: set[str] = set()            # room_names where composite is EGRESS_ACTIVE
_pending_stop: set[str] = set()              # rooms waiting to stop once composite is active
_active_track_egress: set[tuple] = set()      # (room_name, track_sid)
_track_egress_id_to_room: dict[str, str] = {} # track_egress_id → room_name
_ended_egress_ids: set[str] = set()           # dedup
_sse_subscribers: dict[str, list] = {}        # room_name → [Queue]
_finished_rooms: set[str] = set()             # rooms fully destroyed

# Active (human) participants per room — set is idempotent against duplicate webhooks.
# LiveKit fires participant_left twice (two edge IPs); a set handles that safely.
_active_participants: dict[str, set] = {}     # room_name → {identity, ...}

_AUDIO_TYPES = {"AUDIO"}
_AUDIO_SOURCES = {"MICROPHONE", "SCREEN_SHARE_AUDIO"}


def verify_livekit_signature(body: bytes, auth_header: str) -> bool:
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

    if event_type == "room_started":
        # Clear stale state so a reused room name starts fresh
        _finished_rooms.discard(room_name)
        _cleanup_room_state(room_name)
        logger.info(f"🏠 Room started: {room_name} — stale state cleared")
    elif event_type == "track_published":
        asyncio.create_task(_on_track_published(room_name, event))
    elif event_type == "participant_joined":
        asyncio.create_task(_on_participant_joined(room_name, event))
    elif event_type == "participant_left":
        asyncio.create_task(_on_participant_left(room_name, event))
    elif event_type == "egress_updated":
        asyncio.create_task(_on_egress_updated(room_name, event))
    elif event_type == "egress_ended":
        logger.info(f"🏁 Queuing _on_egress_ended for {room_name}")
        asyncio.create_task(_on_egress_ended(room_name, event))
    elif event_type == "room_finished":
        asyncio.create_task(_on_room_finished(room_name))
    else:
        logger.debug(f"⊘ Ignoring event: {event_type}")

    return {"received": True}


async def _on_participant_joined(room_name: str, event: dict) -> None:
    """Track active participants. Egress recorder participants (identity starts with EG_) excluded."""
    participant = event.get("participant", {})
    identity = participant.get("identity", "")

    if identity.startswith("EG_"):
        logger.debug(f"⊘ Egress participant joined (excluded): {identity}")
        return

    _active_participants.setdefault(room_name, set()).add(identity)
    count = len(_active_participants[room_name])
    logger.info(f"👤 Participant joined: {identity} | active={count} | room={room_name}")


async def _on_participant_left(room_name: str, event: dict) -> None:
    """
    Remove participant from active set. When set is empty, stop composite egress.
    Using a set means duplicate participant_left webhooks (LiveKit sends from 2 IPs)
    are idempotent — removing the same identity twice still leaves an empty set.
    """
    participant = event.get("participant", {})
    identity = participant.get("identity", "")

    if identity.startswith("EG_"):
        logger.debug(f"⊘ Egress participant left (excluded): {identity}")
        return

    participants = _active_participants.get(room_name, set())
    participants.discard(identity)
    remaining = len(participants)

    logger.info(f"👤 Participant left: {identity} | remaining={remaining} | room={room_name}")

    if remaining == 0:
        if room_name in _composite_ready:
            logger.info(f"🏁 Last participant left {room_name} — composite active, stopping now")
            asyncio.create_task(_stop_composite_for_room(room_name))
        elif room_name in _active_egress:
            # Composite still starting up — defer stop until EGRESS_ACTIVE
            _pending_stop.add(room_name)
            logger.info(f"⏳ Last participant left {room_name} — composite still starting, deferring stop")


async def _stop_composite_for_room(room_name: str) -> None:
    """
    Stop composite egress while the room is still alive.
    Called immediately when last participant leaves — gives EGRESS_COMPLETE.
    departureTimeout=20s means we have ~20s before room_finished fires.
    """
    egress_id = _active_egress.get(room_name)
    if not egress_id:
        logger.info(f"⊘ No active composite to stop for {room_name}")
        return

    if room_name in _finished_rooms:
        logger.warning(f"⚠️  Room {room_name} already finished — too late to stop cleanly")
        return

    logger.info(f"🛑 Stopping composite {egress_id} for {room_name}")
    try:
        await stop_egress(egress_id=egress_id, room_name=room_name)
        logger.info(f"✅ stop_egress called for {egress_id} — expecting EGRESS_COMPLETE shortly")
    except Exception as e:
        logger.error(f"❌ stop_egress failed for {egress_id}: {e}", exc_info=True)


async def _on_track_published(room_name: str, event: dict) -> None:
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

        is_audio = track_type in _AUDIO_TYPES or track_source in _AUDIO_SOURCES
        if not is_audio:
            logger.info(f"⊘ Non-audio track skipped: type={track_type} source={track_source}")
            return

        logger.info("✅ Audio track confirmed — proceeding with egress")

        if room_name not in _active_egress:
            await _ensure_composite_egress(room_name)

        await _ensure_track_egress(room_name, track_sid, identity)

    except Exception as e:
        logger.error(f"❌ _on_track_published error: {e}", exc_info=True)


async def _ensure_composite_egress(room_name: str) -> None:
    try:
        existing = await list_egress(room_name=room_name)
        running = [
            e for e in existing.get("items", [])
            if e.get("status") in ("EGRESS_STARTING", "EGRESS_ACTIVE")
            and e.get("sourceType") != "EGRESS_SOURCE_TYPE_SDK"
        ]
        if running:
            _active_egress[room_name] = running[0].get("egressId", "")
            logger.info(f"✓ Composite already running: {_active_egress[room_name]}")
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


async def _on_egress_updated(room_name: str, event: dict) -> None:
    egress_info = event.get("egressInfo", {})
    egress_id = egress_info.get("egressId", "")
    status = egress_info.get("status", "")

    # Only care about our composite for this room
    if _active_egress.get(room_name) != egress_id:
        return

    logger.info(f"📡 Composite updated: {egress_id} → {status} | room={room_name}")

    if status == "EGRESS_ACTIVE":
        _composite_ready.add(room_name)
        # If all participants already left, stop now
        if room_name in _pending_stop:
            _pending_stop.discard(room_name)
            logger.info(f"🏁 Composite now active, executing deferred stop for {room_name}")
            asyncio.create_task(_stop_composite_for_room(room_name))


async def _on_egress_ended(room_name: str, event: dict) -> None:
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
        _active_egress.pop(room_name, None)
        _composite_ready.discard(room_name)
        _pending_stop.discard(room_name)

        if status == "EGRESS_COMPLETE":
            logger.info(f"🎬 Composite recording saved for {room_name} ✅")
        else:
            logger.error(
                f"🚨 Composite ABORTED for {room_name} — "
                f"MP4 incomplete. Status: {status}"
            )

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


async def _on_room_finished(room_name: str) -> None:
    """
    Room fully destroyed. By now composite should already be stopped
    (we called stop_egress on participant_left when count hit 0).
    Just clean up state.
    """
    _finished_rooms.add(room_name)

    egress_id = _active_egress.get(room_name)
    if egress_id:
        if room_name in _pending_stop:
            # Composite never reached EGRESS_ACTIVE before room died.
            # Calling stop_egress on a STARTING composite gives ABORTED — don't bother.
            # LiveKit will auto-abort it when the room is gone.
            logger.warning(
                f"⚠️  room_finished while composite {egress_id} still STARTING "
                f"(never reached EGRESS_ACTIVE) — letting LiveKit auto-abort it."
            )
        else:
            # Safety net: composite was ACTIVE but stop_egress call failed earlier.
            logger.warning(
                f"⚠️  room_finished but composite {egress_id} still active — "
                f"stop_egress may have failed earlier. Trying again (may ABORT)."
            )
            try:
                await stop_egress(egress_id=egress_id, room_name=room_name)
            except Exception as e:
                logger.error(f"❌ Safety-net stop_egress failed: {e}", exc_info=True)

    _cleanup_room_state(room_name)
    logger.info(f"🚪 Room finished and cleaned up: {room_name}")


def _cleanup_room_state(room_name: str) -> None:
    _active_participants.pop(room_name, None)
    _active_egress.pop(room_name, None)
    _composite_ready.discard(room_name)
    _pending_stop.discard(room_name)
    stale_track_keys = {k for k in _active_track_egress if k[0] == room_name}
    _active_track_egress.difference_update(stale_track_keys)
    stale_ids = {eid for eid, rn in _track_egress_id_to_room.items() if rn == room_name}
    for eid in stale_ids:
        _track_egress_id_to_room.pop(eid, None)


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
    """Manual stop from /egress/stop endpoint."""
    _cleanup_room_state(room_name)
    _finished_rooms.discard(room_name)
