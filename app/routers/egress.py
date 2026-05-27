from fastapi import APIRouter, HTTPException, Query

from app.config import settings
from app.models import (
    StartEgressRequest, StartEgressResponse,
    StopEgressRequest, StopEgressResponse,
    RecordingUrlResponse, SessionRecording,
    SessionRecordingsResponse, ParticipantRecording,
    ParticipantRecordingsResponse,
)
from app.services.livekit_client import (
    start_composite_egress, stop_egress, list_egress, get_room_participants,
)
from app.services.storage import (
    get_recording_presigned_url,
    check_composite_file,
    list_and_presign_session_files,
    list_and_presign_audio_files,
)
from app.webhooks.handlers import clear_active_egress

router = APIRouter(tags=["Egress"])


@router.post("/egress/start", response_model=StartEgressResponse)
async def egress_start(body: StartEgressRequest):
    """Start composite MP4 recording. Call after participants have published tracks."""
    try:
        result = await start_composite_egress(
            room_name=body.session_id,
            session_id=body.session_id,
            audio_only=body.audio_only,
        )
        return StartEgressResponse(
            egress_id=result.get("egress_id", ""),
            session_id=body.session_id,
            s3_key=result.get("s3_key", ""),
            status=result.get("status", "ACTIVE"),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/egress/stop", response_model=StopEgressResponse)
async def egress_stop(body: StopEgressRequest):
    """Stop a running egress. 412 (already ended) is handled gracefully."""
    try:
        result = await stop_egress(egress_id=body.egress_id, room_name=body.session_id)
        clear_active_egress(body.session_id)
        return StopEgressResponse(egress_id=body.egress_id, status=result.get("status", "ENDED"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/egress/status")
async def egress_status(session_id: str = Query(...)):
    """LiveKit egress state + S3 file availability. Poll until status == 'ready'."""
    livekit_status = "unknown"
    egress_id      = None
    try:
        data  = await list_egress(room_name=session_id)
        items = data.get("items", [])
        if items:
            latest         = items[-1]
            livekit_status = latest.get("status", "unknown")
            egress_id      = latest.get("egressId") or latest.get("egress_id")
        else:
            livekit_status = "no_egress_found"
    except Exception as e:
        livekit_status = f"error: {e}"

    s3_key         = f"TEMP/sessions/{session_id}/composite_recording.mp4"
    content_length = await check_composite_file(session_id)
    s3_ready       = content_length is not None
    size_mb        = round(content_length / (1024 * 1024), 2) if content_length else None

    status = (
        "ready"         if s3_ready else
        "egress_active" if livekit_status in ("EGRESS_STARTING", "EGRESS_ACTIVE") else
        "uploading"     if livekit_status in ("EGRESS_ENDING", "EGRESS_COMPLETE") else
        "failed"        if livekit_status in ("EGRESS_FAILED", "no_egress_found") else
        "processing"
    )
    return {
        "session_id":     session_id,
        "status":         status,
        "livekit_status": livekit_status,
        "egress_id":      egress_id,
        "s3_ready":       s3_ready,
        "size_mb":        size_mb,
        "s3_key":         s3_key,
    }


@router.get("/egress/recording-url", response_model=RecordingUrlResponse)
async def egress_recording_url(
    session_id: str = Query(...),
    expires_in: int = Query(3600, description="Presigned URL TTL in seconds (max 604800)"),
):
    """Presigned S3 URL for the composite MP4. Returns 404 if not ready yet."""
    try:
        result = await get_recording_presigned_url(session_id, expires_in=expires_in)
        return RecordingUrlResponse(session_id=session_id, **result)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/egress/session-recordings", response_model=SessionRecordingsResponse)
async def session_recordings(
    session_id: str = Query(...),
    expires_in: int = Query(3600, description="Presigned URL TTL in seconds (max 604800)"),
):
    """Presigned URLs for all recordings (composite MP4 + per-participant OGG/WebM)."""
    try:
        data = await list_and_presign_session_files(session_id, expires_in)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3 error: {e}")

    c = data["composite"]
    return SessionRecordingsResponse(
        session_id=session_id,
        composite=(
            SessionRecording(kind="composite", identity="", s3_key=c["key"], url=c["url"], expires_in=expires_in)
            if c else None
        ),
        audio=[
            SessionRecording(kind="audio", identity=a["identity"], s3_key=a["key"], url=a["url"], expires_in=expires_in)
            for a in data["audio"]
        ],
        video=[
            SessionRecording(kind="video", identity=v["identity"], s3_key=v["key"], url=v["url"], expires_in=expires_in)
            for v in data["video"]
        ],
        expires_in=expires_in,
    )


@router.get("/egress/participant-recordings")
async def participant_recordings(
    session_id: str = Query(...),
    expires_in: int = Query(3600, description="Presigned URL TTL in seconds"),
):
    """Presigned URLs for all per-participant OGG recordings."""
    file_list = await list_and_presign_audio_files(session_id, expires_in)

    if not file_list:
        raise HTTPException(
            status_code=404,
            detail=f"No per-participant recordings found for session {session_id}.",
        )

    identity_to_role: dict[str, str] = {}
    try:
        participants     = await get_room_participants(session_id)
        identity_to_role = {p["identity"]: p.get("name", p["identity"]) for p in participants}
    except Exception:
        pass

    return ParticipantRecordingsResponse(
        session_id=session_id,
        recordings=[
            ParticipantRecording(
                identity=f["identity"],
                role=identity_to_role.get(f["identity"], f["identity"]),
                s3_key=f["s3_key"],
                url=f["url"],
                expires_in=expires_in,
            )
            for f in file_list
        ],
        expires_in=expires_in,
    )


@router.get("/egress/list")
async def egress_list(session_id: str = Query(...)):
    """List all egress jobs for a session (active + historical)."""
    try:
        return await list_egress(room_name=session_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
