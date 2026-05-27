"""
app/webhooks/handlers.py — LiveKit event handlers + HTTP entry point.

handle_livekit_webhook() is the FastAPI handler. It verifies the JWT
signature then calls dispatch(), which routes each event to the correct
handler. Handlers read/write state exclusively via app.state.session.state
and call services for all I/O.

Event flow:
  room_started       → clear stale state for reused room names
  participant_joined → track active human participants
  participant_left   → decrement count; trigger composite stop when 0
  track_published    → start composite + per-track egress
  egress_updated     → mark composite EGRESS_ACTIVE; execute deferred stop
  egress_ended       → clean up state; broadcast SSE to frontend
  room_finished      → safety-net stop + final cleanup
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging

import jwt as pyjwt
from fastapi import HTTPException, Request

from app.config import settings
from app.services.livekit_client import (
    list_egress,
    start_composite_egress,
    start_track_egress,
    stop_egress,
)
from app.state.session import state

logger = logging.getLogger(__name__)

_AUDIO_TYPES   = {"AUDIO"}
_AUDIO_SOURCES = {"MICROPHONE", "SCREEN_SHARE_AUDIO"}


# ── HTTP entry point ──────────────────────────────────────────────────────────

def verify_livekit_signature(body: bytes, auth_header: str) -> bool:
    try:
        claims = pyjwt.decode(
            auth_header.strip(),
            settings.livekit_api_secret,
            algorithms=["HS256"],
            leeway=5,
        )
        body_hash = base64.b64encode(hashlib.sha256(body).digest()).decode()
        return hmac.compare_digest(claims.get("sha256", ""), body_hash)
    except Exception as e:
        logger.error(f"Webhook signature verification failed: {e}")
        return False


async def handle_livekit_webhook(request: Request) -> dict:
    body        = await request.body()
    auth_header = request.headers.get("Authorization", "")

    logger.info(f"Webhook auth header: {auth_header[:80]}...")
    logger.info(f"Webhook body (first 200): {body[:200]}")

    if not verify_livekit_signature(body, auth_header):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")

    try:
        event = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    event_type = event.get("event", "")
    room_name  = (
        event.get("room", {}).get("name")
        or event.get("egressInfo", {}).get("roomName", "")
    )
    logger.info(f"LiveKit webhook: {event_type} | room={room_name}")
    await dispatch(event_type, room_name, event)
    return {"received": True}


def clear_active_egress(room_name: str) -> None:
    """Reset session state after a manual /egress/stop call."""
    state.cleanup_room(room_name)
    state.unmark_room_finished(room_name)


# ── Dispatcher ────────────────────────────────────────────────────────────────

async def dispatch(event_type: str, room_name: str, event: dict) -> None:
    if event_type == "room_started":
        _on_room_started(room_name)
    elif event_type == "participant_joined":
        asyncio.create_task(_on_participant_joined(room_name, event))
    elif event_type == "participant_left":
        asyncio.create_task(_on_participant_left(room_name, event))
    elif event_type == "track_published":
        asyncio.create_task(_on_track_published(room_name, event))
    elif event_type == "egress_updated":
        asyncio.create_task(_on_egress_updated(room_name, event))
    elif event_type == "egress_ended":
        logger.info(f"🏁 Queuing egress_ended for {room_name}")
        asyncio.create_task(_on_egress_ended(room_name, event))
    elif event_type == "room_finished":
        asyncio.create_task(_on_room_finished(room_name))
    else:
        logger.debug(f"⊘ Ignoring event: {event_type}")


# ── Handlers ──────────────────────────────────────────────────────────────────

def _on_room_started(room_name: str) -> None:
    state.unmark_room_finished(room_name)
    state.cleanup_room(room_name)
    logger.info(f"🏠 Room started: {room_name} — stale state cleared")


async def _on_participant_joined(room_name: str, event: dict) -> None:
    identity = event.get("participant", {}).get("identity", "")
    if identity.startswith("EG_"):
        return
    count = state.add_participant(room_name, identity)
    logger.info(f"👤 Joined: {identity} | active={count} | room={room_name}")


async def _on_participant_left(room_name: str, event: dict) -> None:
    """
    Idempotent: LiveKit sends participant_left from two edge IPs.
    set.discard() handles duplicate removes safely.
    """
    identity = event.get("participant", {}).get("identity", "")
    if identity.startswith("EG_"):
        return
    remaining = state.remove_participant(room_name, identity)
    logger.info(f"👤 Left: {identity} | remaining={remaining} | room={room_name}")

    if remaining == 0:
        if state.is_composite_ready(room_name):
            logger.info(f"🏁 Last participant left — stopping composite for {room_name}")
            asyncio.create_task(_stop_composite_for_room(room_name))
        elif state.has_composite_egress(room_name):
            state.mark_pending_stop(room_name)
            logger.info(f"⏳ Composite still starting — deferring stop for {room_name}")


async def _on_track_published(room_name: str, event: dict) -> None:
    try:
        if not room_name or state.is_room_finished(room_name):
            logger.warning(f"⚠️  track_published ignored for {'missing' if not room_name else 'finished'} room")
            return

        track        = event.get("track", {})
        track_sid    = track.get("sid", "")
        track_type   = str(track.get("type", "")).upper()
        track_source = str(track.get("source", "")).upper()
        identity     = event.get("participant", {}).get("identity", "unknown")

        logger.info(f"📊 Track: type={track_type} source={track_source} sid={track_sid} participant={identity}")

        is_audio = track_type in _AUDIO_TYPES or track_source in _AUDIO_SOURCES
        is_video = track_type == "VIDEO" and track_source == "CAMERA"

        if not is_audio and not is_video:
            return

        track_kind = "audio" if is_audio else "video"
        logger.info(f"✅ {track_kind.upper()} track confirmed")

        if is_audio and not state.has_composite_egress(room_name):
            await _ensure_composite_egress(room_name)

        await _ensure_track_egress(room_name, track_sid, identity, track_kind)

    except Exception as e:
        logger.error(f"❌ _on_track_published error: {e}", exc_info=True)


async def _on_egress_updated(room_name: str, event: dict) -> None:
    egress_info = event.get("egressInfo", {})
    egress_id   = egress_info.get("egressId", "")
    status      = egress_info.get("status", "")

    if state.get_composite_egress(room_name) != egress_id:
        return

    logger.info(f"📡 Composite updated: {egress_id} → {status} | room={room_name}")

    if status == "EGRESS_ACTIVE":
        state.mark_composite_ready(room_name)
        if state.is_pending_stop(room_name):
            state.clear_pending_stop(room_name)
            logger.info(f"🏁 Composite active — executing deferred stop for {room_name}")
            asyncio.create_task(_stop_composite_for_room(room_name))


async def _on_egress_ended(room_name: str, event: dict) -> None:
    egress_info = event.get("egressInfo", {})
    egress_id   = egress_info.get("egressId", "")
    status      = egress_info.get("status", "")

    if state.seen_egress_ended(egress_id):
        logger.info(f"⊘ Duplicate egress_ended ignored: {egress_id}")
        return
    state.mark_egress_ended(egress_id)

    is_composite = state.get_composite_egress(room_name) == egress_id
    is_track     = state.is_track_egress_id(egress_id)

    logger.info(f"Egress ended: {egress_id} | status={status} | composite={is_composite} | track={is_track}")

    if is_composite:
        state.clear_composite_egress(room_name)
        state.clear_composite_ready(room_name)
        state.clear_pending_stop(room_name)

        if status == "EGRESS_COMPLETE":
            logger.info(f"🎬 Composite saved for {room_name} ✅")
        else:
            logger.error(f"🚨 Composite ABORTED for {room_name} — status: {status}")

        for queue in state.get_sse_subscribers(room_name):
            await queue.put({
                "event":      "egress_ended",
                "session_id": room_name,
                "status":     status,
                "success":    status == "EGRESS_COMPLETE",
            })

    if is_track:
        state.remove_track_egress_id(egress_id)
        remaining = state.remaining_track_egress_count(room_name)
        logger.info(f"🎙️  Track done: {egress_id} | remaining for {room_name}: {remaining}")


async def _on_room_finished(room_name: str) -> None:
    state.mark_room_finished(room_name)
    egress_id = state.get_composite_egress(room_name)

    if egress_id:
        if state.is_pending_stop(room_name):
            logger.warning(f"⚠️  room_finished while composite {egress_id} still STARTING — letting LiveKit auto-abort.")
        else:
            logger.warning(f"⚠️  room_finished but composite {egress_id} still active — retrying stop.")
            try:
                await stop_egress(egress_id=egress_id, room_name=room_name)
            except Exception as e:
                logger.error(f"❌ Safety-net stop_egress failed: {e}", exc_info=True)

    _log_session_summary(room_name)
    state.cleanup_room(room_name)
    logger.info(f"🚪 Room finished and cleaned up: {room_name}")


# ── Internal helpers ──────────────────────────────────────────────────────────

async def _stop_composite_for_room(room_name: str) -> None:
    egress_id = state.get_composite_egress(room_name)
    if not egress_id:
        return
    if state.is_room_finished(room_name):
        logger.warning(f"⚠️  Room {room_name} already finished — too late to stop cleanly")
        return
    logger.info(f"🛑 Stopping composite {egress_id} for {room_name}")
    try:
        await stop_egress(egress_id=egress_id, room_name=room_name)
        logger.info(f"✅ stop_egress called for {egress_id}")
    except Exception as e:
        logger.error(f"❌ stop_egress failed for {egress_id}: {e}", exc_info=True)


async def _ensure_composite_egress(room_name: str) -> None:
    try:
        existing = await list_egress(room_name=room_name)
        running  = [
            e for e in existing.get("items", [])
            if e.get("status") in ("EGRESS_STARTING", "EGRESS_ACTIVE")
            and e.get("sourceType") != "EGRESS_SOURCE_TYPE_SDK"
        ]
        if running:
            egress_id = running[0].get("egressId", "")
            state.set_composite_egress(room_name, egress_id)
            logger.info(f"✓ Composite already running: {egress_id}")
            return
        result    = await start_composite_egress(room_name=room_name, session_id=room_name)
        egress_id = result.get("egressId") or result.get("egress_id", "")
        state.set_composite_egress(room_name, egress_id)
        state.set_composite_url(room_name, result.get("s3_url", ""))
        logger.info(f"✅ Composite started: {egress_id}")
    except Exception as e:
        logger.error(f"❌ Composite egress FAILED for {room_name}: {e}", exc_info=True)


async def _ensure_track_egress(
    room_name: str, track_sid: str, identity: str, track_kind: str = "audio"
) -> None:
    if state.has_track_egress(room_name, track_sid):
        return
    try:
        result          = await start_track_egress(room_name, room_name, track_sid, identity, track_kind)
        track_egress_id = result.get("egressId") or result.get("egress_id", "")
        state.add_track_egress(room_name, track_sid, track_egress_id)
        state.set_track_url(room_name, track_kind, identity, result.get("s3_url", ""))
        logger.info(f"✅ {track_kind.upper()} track started: {identity} ({track_sid})")
    except Exception as e:
        logger.error(f"❌ Track egress FAILED for {identity} ({track_sid}): {e}", exc_info=True)


def _log_session_summary(room_name: str) -> None:
    urls = state.get_s3_urls(room_name)
    if not urls:
        return
    lines = [f"📋 Session S3 URLs for {room_name}:"]
    if urls.composite:
        lines.append(f"   🎬 Composite  : {urls.composite}")
    for identity, url in urls.audio.items():
        lines.append(f"   🎙️  Audio [{identity}]: {url}")
    for identity, url in urls.video.items():
        lines.append(f"   🎥 Video [{identity}]: {url}")
    logger.info("\n".join(lines))
