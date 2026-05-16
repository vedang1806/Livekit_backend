"""
webhook.py — LiveKit webhook handler.

Listens for participant_joined events and auto-starts egress when the
first participant enters a room. Verifies the LiveKit signature on every
incoming request to reject forged payloads.
"""

import hashlib
import hmac
import json
import logging

from fastapi import Request, HTTPException

from config import settings  # noqa: used in log strings
from egress import start_composite_egress, start_track_egress, list_egress

logger = logging.getLogger(__name__)

# Composite egress per room — prevents duplicate room-level recordings.
_active_egress: dict[str, str] = {}          # room_name → egress_id

# Track-level egresses — prevents duplicate per-participant audio recordings.
# Key: (room_name, track_sid)
_active_track_egress: set[tuple] = set()


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
    room        = event.get("room", {})
    room_name   = room.get("name", "")

    logger.info(f"LiveKit webhook: {event_type} | room={room_name}")

    if event_type == "participant_joined":
        await _on_participant_joined(room_name, event)

    elif event_type == "track_published":
        await _on_track_published(room_name, event)

    elif event_type == "egress_ended":
        egress_id = event.get("egressInfo", {}).get("egressId", "")
        _active_egress.pop(room_name, None)
        # Clear any track egresses for this room from the guard set
        stale = {key for key in _active_track_egress if key[0] == room_name}
        _active_track_egress.difference_update(stale)
        logger.info(f"Egress ended: {egress_id} | room={room_name}")

    return {"received": True}


async def _on_participant_joined(room_name: str, event: dict) -> None:
    if not room_name:
        return

    # Already recording this room — skip.
    if room_name in _active_egress:
        logger.info(f"Egress already active for {room_name} — skipping auto-start")
        return

    # Guard: check LiveKit too in case the server restarted and lost in-memory state.
    try:
        existing = await list_egress(room_name=room_name)
        running = [
            e for e in existing.get("items", [])
            if e.get("status") in ("EGRESS_STARTING", "EGRESS_ACTIVE")
        ]
        if running:
            egress_id = running[0].get("egressId", "unknown")
            _active_egress[room_name] = egress_id
            logger.info(f"Found existing egress {egress_id} for {room_name} — skipping auto-start")
            return
    except Exception as e:
        logger.warning(f"Could not check existing egress for {room_name}: {e}")

    participant = event.get("participant", {})
    identity    = participant.get("identity", "unknown")
    logger.info(f"First participant '{identity}' joined {room_name} — auto-starting egress")

    try:
        result = await start_composite_egress(
            room_name=room_name,
            session_id=room_name,
            audio_only=False,
        )
        egress_id = result.get("egress_id", "")
        _active_egress[room_name] = egress_id
        logger.info(f"Auto-egress started: {egress_id} | room={room_name}")
        logger.info(f"Recording URL: {result.get('s3_url', '')}")
    except Exception as e:
        logger.error(f"Auto-egress start failed for {room_name}: {e}")


async def _on_track_published(room_name: str, event: dict) -> None:
    """Auto-start a track egress for every audio track that gets published."""
    if not room_name:
        return

    track       = event.get("track", {})
    track_sid   = track.get("sid", "")
    track_type  = track.get("type", "")   # "AUDIO" | "VIDEO" | "DATA" or int 0/1/2
    participant = event.get("participant", {})
    identity    = participant.get("identity", "unknown")

    # Only record audio tracks (type == "AUDIO" or 0)
    if track_type not in ("AUDIO", 0):
        logger.info(f"Skipping non-audio track {track_sid} (type={track_type}) for {identity}")
        return

    key = (room_name, track_sid)
    if key in _active_track_egress:
        logger.info(f"Track egress already running for {track_sid} — skipping")
        return

    logger.info(f"Audio track published: {track_sid} | participant={identity} | room={room_name}")

    try:
        result = await start_track_egress(
            room_name=room_name,
            session_id=room_name,
            track_sid=track_sid,
            identity=identity,
        )
        _active_track_egress.add(key)
        logger.info(f"Audio OGG recording → s3://{settings.s3_bucket}/{result.get('s3_key', '')}")
        logger.info(f"Audio OGG presigned  → (call /egress/recording-url?session_id={room_name}&track={identity} after session ends)")
    except Exception as e:
        logger.error(f"Track egress failed for {identity} ({track_sid}): {e}")


def clear_active_egress(room_name: str) -> None:
    """Call this from /egress/stop so the in-memory set stays consistent."""
    _active_egress.pop(room_name, None)
    stale = {key for key in _active_track_egress if key[0] == room_name}
    _active_track_egress.difference_update(stale)
