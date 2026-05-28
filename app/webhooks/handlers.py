"""
app/webhooks/handlers.py — LiveKit event handlers + HTTP entry point.

Each handler writes to BOTH in-memory state (for live session coordination)
AND the database (for persistence, admin panel, compliance checks).
DB failures are logged but never break live session behavior.

Event flow:
  room_started       → reset in-memory state + create/reset DB session
  participant_joined → track in state + add DB participant row
  participant_left   → decrement state + mark DB participant left
  track_published    → start composite + per-track egress (state + DB)
  egress_updated     → mark composite ACTIVE in state + update DB status
  egress_ended       → clean state + update DB + dispatch compliance task
  room_finished      → safety-net stop + end DB session + cleanup
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
from app.db.models import (
    ComplianceReport,
    ComplianceStatus,
    EgressStatus,
    EgressType,
    ParticipantRole,
)
from app.db.session import AsyncSessionLocal
from app.repositories.egress_repo import EgressRepository
from app.repositories.session_repo import SessionRepository
from app.repositories.webhook_repo import WebhookRepository
from app.services.livekit_client import (
    list_egress,
    start_composite_egress,
    start_track_egress,
    stop_egress,
)
from app.state.session import state
from app.workers.tasks import run_compliance_check

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

    # Persist raw webhook for audit log (fire-and-forget, never blocks)
    asyncio.create_task(_log_webhook(event_type, room_name, event))

    await dispatch(event_type, room_name, event)
    return {"received": True}


def clear_active_egress(room_name: str) -> None:
    """Reset in-memory session state after a manual /egress/stop call."""
    state.cleanup_room(room_name)
    state.unmark_room_finished(room_name)


# ── Dispatcher ────────────────────────────────────────────────────────────────

async def dispatch(event_type: str, room_name: str, event: dict) -> None:
    if event_type == "room_started":
        # Awaited (not create_task) so the DB session row exists before
        # participant_joined / track_published tasks try to reference it.
        await _on_room_started(room_name)
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

async def _on_room_started(room_name: str) -> None:
    # In-memory reset
    state.unmark_room_finished(room_name)
    state.cleanup_room(room_name)
    logger.info(f"🏠 Room started: {room_name} — stale state cleared")

    # DB: end any existing active session for this room name, then create fresh one
    try:
        async with AsyncSessionLocal() as db:
            repo = SessionRepository(db)
            await repo.end_session(room_name)   # no-op if none exists
            await repo.get_or_create(room_name)
            await db.commit()
    except Exception as e:
        logger.error(f"❌ DB error in _on_room_started({room_name}): {e}", exc_info=True)


async def _on_participant_joined(room_name: str, event: dict) -> None:
    identity = event.get("participant", {}).get("identity", "")
    if identity.startswith("EG_"):
        return

    # In-memory
    count = state.add_participant(room_name, identity)
    logger.info(f"👤 Joined: {identity} | active={count} | room={room_name}")

    # DB
    try:
        role = _infer_role(identity)
        async with AsyncSessionLocal() as db:
            session_repo = SessionRepository(db)
            session      = await session_repo.get_or_create(room_name)
            await session_repo.add_participant(session.id, identity, role)
            await db.commit()
    except Exception as e:
        logger.error(f"❌ DB error in _on_participant_joined({identity}): {e}", exc_info=True)


async def _on_participant_left(room_name: str, event: dict) -> None:
    """
    Idempotent: LiveKit sends participant_left from two edge IPs.
    set.discard() handles duplicate removes safely.
    """
    identity = event.get("participant", {}).get("identity", "")
    if identity.startswith("EG_"):
        return

    # In-memory
    remaining = state.remove_participant(room_name, identity)
    logger.info(f"👤 Left: {identity} | remaining={remaining} | room={room_name}")

    # DB
    try:
        async with AsyncSessionLocal() as db:
            session_repo = SessionRepository(db)
            session      = await session_repo.get_active(room_name)
            if session:
                await session_repo.mark_participant_left(session.id, identity)
                await db.commit()
    except Exception as e:
        logger.error(f"❌ DB error in _on_participant_left({identity}): {e}", exc_info=True)

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

    # In-memory
    if status == "EGRESS_ACTIVE":
        state.mark_composite_ready(room_name)
        if state.is_pending_stop(room_name):
            state.clear_pending_stop(room_name)
            logger.info(f"🏁 Composite active — executing deferred stop for {room_name}")
            asyncio.create_task(_stop_composite_for_room(room_name))

    # DB
    if status in ("EGRESS_ACTIVE", "EGRESS_STARTING"):
        db_status = EgressStatus.active if status == "EGRESS_ACTIVE" else EgressStatus.starting
        try:
            async with AsyncSessionLocal() as db:
                await EgressRepository(db).update_status(egress_id, db_status)
                await db.commit()
        except Exception as e:
            logger.error(f"❌ DB error in _on_egress_updated({egress_id}): {e}", exc_info=True)


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

    # DB: update egress status + attach recording + store S3 URL on session/participant
    db_status    = EgressStatus.complete if status == "EGRESS_COMPLETE" else EgressStatus.aborted
    file_results = egress_info.get("fileResults", [])
    s3_key       = file_results[0].get("filename", "") if file_results else ""

    try:
        async with AsyncSessionLocal() as db:
            egress_repo  = EgressRepository(db)
            session_repo = SessionRepository(db)

            job = await egress_repo.update_status(egress_id, db_status)

            if job and s3_key and status == "EGRESS_COMPLETE":
                # Attach recording file row
                file_type = s3_key.rsplit(".", 1)[-1] if "." in s3_key else "unknown"
                await egress_repo.add_recording(job.id, s3_key, file_type)

                if is_composite:
                    # Store composite URL on the session row
                    await session_repo.set_composite_s3_url(room_name, s3_key)

                elif is_track and job.identity:
                    # Store track URL on the participant row
                    session = await session_repo.get_active(room_name)
                    if session:
                        await session_repo.set_participant_track_s3_url(session.id, job.identity, s3_key)

            await db.commit()
    except Exception as e:
        logger.error(f"❌ DB error in _on_egress_ended({egress_id}): {e}", exc_info=True)

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

        # Only run compliance check on the interpreter's video track
        if status == "EGRESS_COMPLETE":
            await _dispatch_compliance_check(egress_id, room_name, event)


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

    # DB: mark session ended
    try:
        async with AsyncSessionLocal() as db:
            await SessionRepository(db).end_session(room_name)
            await db.commit()
    except Exception as e:
        logger.error(f"❌ DB error in _on_room_finished({room_name}): {e}", exc_info=True)


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

        # DB: create EgressJob row
        try:
            async with AsyncSessionLocal() as db:
                session_repo = SessionRepository(db)
                egress_repo  = EgressRepository(db)
                session      = await session_repo.get_or_create(room_name)
                await egress_repo.create_egress(session.id, egress_id, EgressType.composite)
                await db.commit()
        except Exception as e:
            logger.error(f"❌ DB error saving composite egress({egress_id}): {e}", exc_info=True)

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

        # DB: create EgressJob row
        try:
            async with AsyncSessionLocal() as db:
                session_repo = SessionRepository(db)
                egress_repo  = EgressRepository(db)
                session      = await session_repo.get_or_create(room_name)
                await egress_repo.create_egress(
                    session.id, track_egress_id, EgressType.track,
                    track_sid=track_sid, identity=identity,
                )
                await db.commit()
        except Exception as e:
            logger.error(f"❌ DB error saving track egress({track_egress_id}): {e}", exc_info=True)

    except Exception as e:
        logger.error(f"❌ Track egress FAILED for {identity} ({track_sid}): {e}", exc_info=True)


async def _dispatch_compliance_check(egress_id: str, room_name: str, event: dict) -> None:
    """
    Dispatch compliance check ONLY for the interpreter's track egress.
    Skips silently for doctor/patient/unknown tracks.
    Expected face count is always 1 — only the interpreter should be in their own frame.
    """
    try:
        egress_info  = event.get("egressInfo", {})
        file_results = egress_info.get("fileResults", [])
        s3_key       = file_results[0].get("filename", "") if file_results else ""

        if not s3_key:
            logger.warning(f"⚠️  No S3 key in egress event for {egress_id} — skipping")
            return

        async with AsyncSessionLocal() as db:
            job = await EgressRepository(db).get_by_egress_id(egress_id)
            if not job:
                logger.error(f"EgressJob not found for {egress_id}")
                return

            identity = job.identity or ""
            # Only process interpreter tracks
            if _infer_role(identity) != ParticipantRole.interpreter:
                logger.info(f"⊘ Skipping compliance for non-interpreter track: {identity}")
                return

            report = ComplianceReport(
                egress_job_id=job.id,
                status=ComplianceStatus.pending,
                participant_identity=identity,
                s3_url=s3_key,
                expected_face_count=1,  # interpreter should always be alone in their frame
            )
            db.add(report)
            await db.commit()
            await db.refresh(report)

        run_compliance_check.apply_async(
            kwargs={
                "egress_job_id":             report.id,
                "s3_key":                    s3_key,
                "participant_identity":       identity,
                "expected_participant_count": 1,
            },
            queue="compliance",
        )
        logger.info(f"🔍 Compliance queued for interpreter={identity} | egress={egress_id}")

    except Exception as e:
        logger.error(f"❌ Failed to dispatch compliance check for {egress_id}: {e}", exc_info=True)


async def _log_webhook(event_type: str, room_name: str, payload: dict) -> None:
    try:
        async with AsyncSessionLocal() as db:
            await WebhookRepository(db).log(event_type, room_name, payload)
            await db.commit()
    except Exception as e:
        logger.error(f"❌ DB error logging webhook({event_type}): {e}", exc_info=True)


def _infer_role(identity: str) -> ParticipantRole:
    """Best-effort role from identity string (e.g. 'doctor-123' → doctor)."""
    lower = identity.lower()
    if "doctor" in lower or "physician" in lower:
        return ParticipantRole.doctor
    if "patient" in lower:
        return ParticipantRole.patient
    if "interpreter" in lower or "interp" in lower:
        return ParticipantRole.interpreter
    return ParticipantRole.unknown


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
