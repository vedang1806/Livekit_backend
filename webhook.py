"""
webhook.py — LiveKit webhook handler.

Listens for track_published and egress_ended events, auto-starts egress when
audio tracks are published. Verifies the LiveKit signature on every incoming
request to reject forged payloads.

BEST PRACTICE:
- Accept all webhook events (LiveKit sends everything)
- Filter unwanted events immediately
- Return 200 OK as fast as possible
- Queue egress operations for async processing
"""

import asyncio
import hashlib
import hmac
import json
import logging

from fastapi import Request, HTTPException

from config import settings  # noqa: used in log strings
from egress import start_composite_egress, start_track_egress, list_egress, stop_egress

logger = logging.getLogger(__name__)

# Composite egress per room — prevents duplicate room-level recordings.
_active_egress: dict[str, str] = {}          # room_name → composite_egress_id

# Track-level egresses — prevents duplicate per-participant audio recordings.
# Key: (room_name, track_sid)
_active_track_egress: set[tuple] = set()

# Maps track egress_id → room_name so we know when ALL track egresses for a
# room are done and can stop the composite before the room is destroyed.
_track_egress_id_to_room: dict[str, str] = {}

# Cancelable delayed composite-stop tasks — keyed by room_name.
# Cancelled if a new participant publishes audio before the delay expires.
_pending_composite_stop: dict[str, asyncio.Task] = {}

# Deduplication: egress IDs we've already processed egress_ended for.
_ended_egress_ids: set[str] = set()

# SSE queues — frontend subscribers waiting for egress_ended per session.
# room_name → list of asyncio.Queue
_sse_subscribers: dict[str, list] = {}


def verify_livekit_signature(body: bytes, auth_header: str) -> bool:
    """
    LiveKit sends a JWT signed with the API secret.
    The JWT payload 'sha256' field is base64-encoded SHA256 of the request body.
    """
    import jwt as pyjwt
    import base64
    try:
        token = auth_header.strip()
        claims = pyjwt.decode(
            token,
            settings.livekit_api_secret,
            algorithms=["HS256"],
            leeway=30,  # tolerate up to 30s clock skew
        )
        body_hash_b64 = base64.b64encode(hashlib.sha256(body).digest()).decode()
        jwt_hash      = claims.get("sha256", "")
        return hmac.compare_digest(jwt_hash, body_hash_b64)
    except Exception as e:
        logger.error(f"Webhook signature verification failed: {e}")
        return False


async def handle_livekit_webhook(request: Request) -> dict:
    """
    Accept webhook, verify signature, and return 200 OK immediately.
    Queue event processing asynchronously to prevent backlog.
    """
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
    room       = event.get("room", {})
    # egress_ended puts room name inside egressInfo.roomName, not room.name
    room_name  = (
        room.get("name")
        or event.get("egressInfo", {}).get("roomName", "")
    )

    logger.info(f"LiveKit webhook: {event_type} | room={room_name}")

    # ✅ Filter: Only care about track_published and egress_ended
    if event_type == "track_published":
        logger.info(f"🔊 Queuing _on_track_published for {room_name}")
        # Queue async processing and return 200 OK immediately
        asyncio.create_task(_on_track_published(room_name, event))
    elif event_type == "egress_ended":
        logger.info(f"🏁 Queuing _on_egress_ended for {room_name}")
        asyncio.create_task(_on_egress_ended(room_name, event))
    elif event_type == "room_finished":
        logger.info(f"🚪 Queuing _on_room_finished for {room_name}")
        asyncio.create_task(_on_room_finished(room_name))
    else:
        # Drop all other events immediately
        logger.debug(f"⊘ Ignoring event type: {event_type}")

    # ✅ Return 200 OK immediately (don't wait for async tasks)
    return {"received": True}




async def _on_track_published(room_name: str, event: dict) -> None:
    """
    Triggered by track_published webhook (subscribe in LiveKit Cloud dashboard).
    On every audio track:
      1. Start composite room egress once (first audio track in the room)
      2. Start per-participant OGG track egress
    track_published fires after the track is live — no delay needed, zero data loss.

    Filters for audio tracks:
    - type == "AUDIO" OR source == "MICROPHONE" or "SCREEN_SHARE_AUDIO"
    """
    try:
        if not room_name:
            logger.warning("⚠️  _on_track_published: room_name is empty!")
            return

        track       = event.get("track", {})
        track_sid   = track.get("sid", "")
        track_type  = track.get("type", "")
        track_source = track.get("source", "")
        participant = event.get("participant", {})
        identity    = participant.get("identity", "unknown")

        logger.info(f"📊 Track published: type={track_type}, source={track_source}, sid={track_sid}, participant={identity}")

        # Check if this is an audio track
        # type=="AUDIO" OR source is "MICROPHONE" or "SCREEN_SHARE_AUDIO"
        is_audio_by_type = track_type in ("AUDIO", 0, 2)
        is_audio_by_source = track_source in ("MICROPHONE", "SCREEN_SHARE_AUDIO", 2, 4)
        is_audio = is_audio_by_type or is_audio_by_source

        if not is_audio:
            logger.info(f"⊘ Skipping non-audio track: type={track_type}, source={track_source}")
            return

        logger.info(f"✅ Audio track detected! Starting egress...")

        # ── 1. Composite egress (once per room) ───────────────────────────────────
        if room_name not in _active_egress:
            try:
                logger.info(f"🔎 Checking for existing egress in {room_name}")
                existing = await list_egress(room_name=room_name)
                running  = [
                    e for e in existing.get("items", [])
                    if e.get("status") in ("EGRESS_STARTING", "EGRESS_ACTIVE")
                ]
                if running:
                    _active_egress[room_name] = running[0].get("egressId", "")
                    logger.info(f"✓ Composite egress already running for {room_name} — skipping")
                else:
                    logger.info(f"▶️  Starting composite egress for {room_name}")
                    result = await start_composite_egress(
                        room_name=room_name,
                        session_id=room_name,
                        audio_only=False,
                    )
                    _active_egress[room_name] = result.get("egressId") or result.get("egress_id", "")
                    logger.info(f"✅ Composite egress started: {_active_egress[room_name]} | room={room_name}")
                    logger.info(f"📁 Recording → {result.get('s3_url', '')}")
            except Exception as e:
                logger.error(f"❌ Composite egress start FAILED for {room_name}: {e}", exc_info=True)

        # ── 2. Per-participant OGG ─────────────────────────────────────────────────
        key = (room_name, track_sid)
        if key in _active_track_egress:
            logger.info(f"⊘ OGG already recording for {track_sid}")
            return

        # Cancel any pending composite stop — a new participant is joining.
        pending = _pending_composite_stop.pop(room_name, None)
        if pending:
            pending.cancel()
            logger.info(f"🔄 New track in {room_name}, cancelled pending composite stop")

        logger.info(f"🎙️  Starting OGG for participant: {identity} ({track_sid})")
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
            logger.info(f"✅ OGG recording started → s3://{settings.s3_bucket}/{result.get('s3_key', '')}")
        except Exception as e:
            logger.error(f"❌ Track egress FAILED for {identity} ({track_sid}): {e}", exc_info=True)
    except Exception as e:
        logger.error(f"❌ Error in _on_track_published: {e}", exc_info=True)



async def _on_egress_ended(room_name: str, event: dict) -> None:
    egress_info = event.get("egressInfo", {})
    egress_id   = egress_info.get("egressId", "")
    status      = egress_info.get("status", "")

    # Deduplicate: LiveKit occasionally sends the same egress_ended twice.
    if egress_id in _ended_egress_ids:
        logger.info(f"Duplicate egress_ended ignored: {egress_id}")
        return
    _ended_egress_ids.add(egress_id)

    is_composite = (_active_egress.get(room_name) == egress_id)
    is_track     = (egress_id in _track_egress_id_to_room)

    if is_composite:
        _active_egress.pop(room_name, None)

    if is_track:
        _track_egress_id_to_room.pop(egress_id, None)
        # Check if all track egresses for this room are now done.
        remaining = [eid for eid, rn in _track_egress_id_to_room.items() if rn == room_name]
        if not remaining and room_name in _active_egress:
            composite_id = _active_egress[room_name]
            logger.info(f"🏁 Last track egress done for {room_name} — scheduling composite stop in 10s")
            task = asyncio.create_task(_delayed_stop_composite(room_name, composite_id))
            _pending_composite_stop[room_name] = task

    logger.info(f"Egress ended: {egress_id} | room={room_name} | status={status}")

    # Notify SSE subscribers only when composite ends (that's the signal the session is fully saved).
    if is_composite:
        for queue in _sse_subscribers.get(room_name, []):
            await queue.put({"event": "egress_ended", "session_id": room_name, "status": status})


async def _stop_composite(room_name: str, egress_id: str) -> None:
    """Stop the composite egress so LiveKit finalizes the MP4 cleanly (EGRESS_COMPLETE)."""
    logger.info(f"🛑 Stopping composite {egress_id} for room {room_name}")
    try:
        await stop_egress(egress_id=egress_id, room_name=room_name)
    except Exception as e:
        logger.error(f"❌ Failed to stop composite egress {egress_id}: {e}", exc_info=True)


async def _delayed_stop_composite(room_name: str, egress_id: str, delay: float = 10.0) -> None:
    """
    Stop composite after `delay` seconds, unless cancelled by a new participant.
    Must be called while the room is still alive (before room_finished) so LiveKit
    can finalize the MP4 cleanly. departureTimeout=20s gives us the window.
    """
    await asyncio.sleep(delay)
    _pending_composite_stop.pop(room_name, None)
    # Guard: composite may have already ended (e.g. deduplication or restart)
    if _active_egress.get(room_name) != egress_id:
        logger.info(f"⊘ Delayed composite stop skipped for {room_name} — already cleared")
        return
    await _stop_composite(room_name, egress_id)


async def _on_room_finished(room_name: str) -> None:
    """
    Clean up in-memory state when the room is destroyed.
    Do NOT call stop_egress here — by the time room_finished fires the room is
    being torn down, and any stop produces EGRESS_ABORTED instead of EGRESS_COMPLETE.
    The composite should already be stopped by _delayed_stop_composite.
    """
    # Cancel the delayed stop if it hasn't fired yet (room gone, nothing to finalize).
    pending = _pending_composite_stop.pop(room_name, None)
    if pending:
        pending.cancel()
        logger.info(f"🚪 Room finished: cancelled pending composite stop for {room_name}")

    _active_egress.pop(room_name, None)
    stale_tracks = {key for key in _active_track_egress if key[0] == room_name}
    _active_track_egress.difference_update(stale_tracks)
    stale_ids = {eid for eid, rn in _track_egress_id_to_room.items() if rn == room_name}
    for eid in stale_ids:
        _track_egress_id_to_room.pop(eid, None)
    logger.info(f"🚪 Room finished: state cleared for {room_name}")


def subscribe_sse(room_name: str) -> asyncio.Queue:
    """Register a new SSE subscriber for a room. Returns a queue to read events from."""
    q = asyncio.Queue()
    _sse_subscribers.setdefault(room_name, []).append(q)
    return q


def unsubscribe_sse(room_name: str, queue: asyncio.Queue) -> None:
    subs = _sse_subscribers.get(room_name, [])
    if queue in subs:
        subs.remove(queue)


def clear_active_egress(room_name: str) -> None:
    """Call this from /egress/stop so the in-memory state stays consistent."""
    pending = _pending_composite_stop.pop(room_name, None)
    if pending:
        pending.cancel()
    _active_egress.pop(room_name, None)
    stale_tracks = {key for key in _active_track_egress if key[0] == room_name}
    _active_track_egress.difference_update(stale_tracks)
    stale_ids = {eid for eid, rn in _track_egress_id_to_room.items() if rn == room_name}
    for eid in stale_ids:
        _track_egress_id_to_room.pop(eid, None)
