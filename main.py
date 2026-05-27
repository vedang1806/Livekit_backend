"""
interpreter-backend — main.py
FastAPI entry point: token minting, room management, health check.
"""

import asyncio
import json
import logging
import uvicorn

from contextlib import asynccontextmanager
from botocore.exceptions import ClientError as BotoClientError
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import StreamingResponse

logging.basicConfig(level=logging.INFO)

from config import settings
from tokens import generate_participant_token
from webhook import (
    handle_livekit_webhook,
    clear_active_egress,
    subscribe_sse,
    unsubscribe_sse,
    cleanup_stale_state,
)
from egress import (
    create_room,
    start_composite_egress,
    stop_egress,
    list_egress,
    get_room_participants,
    get_recording_presigned_url,
    init_http_client,
    close_http_client,
    s3_client,
)
from models import (
    CreateRoomRequest,
    CreateRoomResponse,
    TokenResponse,
    StartEgressRequest,
    StartEgressResponse,
    StopEgressRequest,
    StopEgressResponse,
    ParticipantsResponse,
    RecordingUrlResponse,
    ParticipantRecordingsResponse,
    SessionRecording,
    SessionRecordingsResponse,
    HealthResponse,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_http_client()
    gc_task = asyncio.create_task(cleanup_stale_state())

    print(f"✅ Interpreter Backend starting")
    print(f"   LiveKit : {settings.livekit_url}")
    print(f"   S3      : s3://{settings.s3_bucket} ({settings.aws_region})")

    yield

    gc_task.cancel()
    try:
        await gc_task
    except asyncio.CancelledError:
        pass
    await close_http_client()
    print("Interpreter Backend shutting down.")


app = FastAPI(
    title="Interpreter Backend",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── SSE ───────────────────────────────────────────────────────────────────────

@app.get("/egress/events", tags=["egress"])
async def egress_events(session_id: str = Query(...)):
    """
    Server-Sent Events stream — fires once when egress ends for this session.
    Frontend connects here after session starts and waits for the 'egress_ended' event,
    then fetches /egress/recording-url to get the presigned link.
    """
    queue = subscribe_sse(session_id)

    async def stream():
        try:
            yield "data: {\"event\": \"connected\"}\n\n"
            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=300)
                    yield f"data: {json.dumps(msg)}\n\n"
                    if msg.get("event") == "egress_ended":
                        break
                except asyncio.TimeoutError:
                    yield "data: {\"event\": \"keepalive\"}\n\n"
        finally:
            unsubscribe_sse(session_id, queue)

    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["system"])
async def health():
    return HealthResponse(
        status="ok",
        livekit_url=settings.livekit_url,
        s3_bucket=settings.s3_bucket,
    )


# ── Room ──────────────────────────────────────────────────────────────────────

@app.post("/room/create", response_model=CreateRoomResponse, tags=["room"])
async def room_create(body: CreateRoomRequest):
    """
    Create a LiveKit room for a new interpreter session.
    Returns room metadata + admin token for server operations.
    """
    try:
        room = await create_room(body.session_id)
        return CreateRoomResponse(
            session_id=body.session_id,
            room_sid=room.get("sid", ""),
            livekit_url=settings.livekit_url,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Token ─────────────────────────────────────────────────────────────────────

@app.get("/token", response_model=TokenResponse, tags=["token"])
async def get_token(
    room: str     = Query(..., description="LiveKit room / session_id"),
    identity: str = Query(..., description="Unique participant identity string"),
    role: str     = Query(..., description="patient | doctor | interpreter"),
):
    """
    Mint a participant JWT for the LiveKit frontend SDK.
    The role is embedded as the display name so LiveKit renders it on the tile.
    """
    VALID_ROLES = {"patient", "doctor", "interpreter"}
    if role not in VALID_ROLES:
        raise HTTPException(status_code=400, detail=f"role must be one of {VALID_ROLES}")

    display_name = role.upper()
    token = generate_participant_token(
        room_name=room,
        identity=identity,
        display_name=display_name,
        api_key=settings.livekit_api_key,
        api_secret=settings.livekit_api_secret,
    )
    return TokenResponse(
        token=token,
        url=settings.livekit_url,
        identity=identity,
        role=role,
        room=room,
    )


# ── Egress ────────────────────────────────────────────────────────────────────

@app.post("/egress/start", response_model=StartEgressResponse, tags=["egress"])
async def egress_start(body: StartEgressRequest):
    """
    Start a composite video egress for the session.
    Records all participants in a grid layout with name overlays → S3 as MP4.
    Call AFTER participants have joined and published their tracks.
    """
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


@app.post("/webhook/livekit", tags=["system"])
async def livekit_webhook(request: Request):
    """
    Receives LiveKit server-side events.
    Auto-starts egress when the first participant joins a room.
    Configure this URL in your LiveKit Cloud dashboard → Webhooks.
    """
    return await handle_livekit_webhook(request)


@app.post("/egress/stop", response_model=StopEgressResponse, tags=["egress"])
async def egress_stop(body: StopEgressRequest):
    """
    Stop a running egress job.
    Handles 412 (already ended/aborted) gracefully — not treated as an error.
    """
    try:
        result = await stop_egress(
            egress_id=body.egress_id,
            room_name=body.session_id,
        )
        clear_active_egress(body.session_id)
        return StopEgressResponse(
            egress_id=body.egress_id,
            status=result.get("status", "ENDED"),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/egress/status", tags=["egress"])
async def egress_status(session_id: str = Query(...)):
    """
    Returns both LiveKit egress state AND S3 file availability.
    Use this to know when recording is ready — poll until status == 'ready'.
    """
    # 1. LiveKit egress state (async — non-blocking)
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

    # 2. S3 file check — offload blocking boto3 I/O to thread pool
    s3_key = f"TEMP/sessions/{session_id}/composite_recording.mp4"

    def _check_s3():
        try:
            head = s3_client.head_object(Bucket=settings.s3_bucket, Key=s3_key)
            return head["ContentLength"]
        except BotoClientError:
            return None

    content_length = await asyncio.to_thread(_check_s3)
    s3_ready = content_length is not None
    size_mb  = round(content_length / (1024 * 1024), 2) if content_length else None

    status = "ready"          if s3_ready else (
        "egress_active"       if livekit_status in ("EGRESS_STARTING", "EGRESS_ACTIVE") else
        "uploading"           if livekit_status in ("EGRESS_ENDING", "EGRESS_COMPLETE") else
        "failed"              if livekit_status in ("EGRESS_FAILED", "no_egress_found") else
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


@app.get("/egress/recording-url", response_model=RecordingUrlResponse, tags=["egress"])
async def egress_recording_url(
    session_id: str = Query(...),
    expires_in: int = Query(3600, description="Presigned URL TTL in seconds (max 604800)"),
):
    """
    Get a presigned S3 URL to view/download the session recording.
    The URL is time-limited — default 1 hour, max 7 days.

    Returns 404 if the recording is still being processed (egress running or uploading).
    Retry after ~10–30s for the file to be ready.
    """
    try:
        result = await get_recording_presigned_url(session_id, expires_in=expires_in)
        return RecordingUrlResponse(session_id=session_id, **result)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/egress/session-recordings", response_model=SessionRecordingsResponse, tags=["egress"])
async def session_recordings(
    session_id: str = Query(...),
    expires_in: int = Query(3600, description="Presigned URL TTL in seconds (max 604800)"),
):
    """
    Returns presigned S3 URLs for ALL recordings from a session in one call:
      - composite: the full grid MP4 (TEMP/sessions/{id}/composite_recording.mp4)
      - audio[]:   per-participant OGG files  (TEMP/sessions/{id}/audio/{identity}.ogg)
      - video[]:   per-participant WebM files (TEMP/sessions/{id}/video/{identity}.webm)

    Call this after egress has ended. Any missing file type is simply omitted rather
    than returning a 404 — check the composite / audio / video fields individually.
    """
    prefix = f"TEMP/sessions/{session_id}/"

    def _list_and_presign():
        paginator = s3_client.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=settings.s3_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])

        results: dict = {"composite": None, "audio": [], "video": []}
        for key in keys:
            tail = key[len(prefix):]
            try:
                url = s3_client.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": settings.s3_bucket, "Key": key},
                    ExpiresIn=expires_in,
                )
            except BotoClientError:
                continue

            if tail == "composite_recording.mp4":
                results["composite"] = {"key": key, "url": url}
            elif tail.startswith("audio/") and tail.endswith(".ogg"):
                results["audio"].append({"key": key, "url": url, "identity": tail[len("audio/"):-len(".ogg")]})
            elif tail.startswith("video/") and tail.endswith(".webm"):
                results["video"].append({"key": key, "url": url, "identity": tail[len("video/"):-len(".webm")]})

        return results

    try:
        data = await asyncio.to_thread(_list_and_presign)
    except BotoClientError as e:
        raise HTTPException(status_code=500, detail=f"S3 list error: {e}")

    c = data["composite"]
    composite_rec = (
        SessionRecording(kind="composite", identity="", s3_key=c["key"], url=c["url"], expires_in=expires_in)
        if c else None
    )
    audio_recs = [
        SessionRecording(kind="audio", identity=a["identity"], s3_key=a["key"], url=a["url"], expires_in=expires_in)
        for a in data["audio"]
    ]
    video_recs = [
        SessionRecording(kind="video", identity=v["identity"], s3_key=v["key"], url=v["url"], expires_in=expires_in)
        for v in data["video"]
    ]

    return SessionRecordingsResponse(
        session_id=session_id,
        composite=composite_rec,
        audio=audio_recs,
        video=video_recs,
        expires_in=expires_in,
    )


@app.get("/egress/participant-recordings", tags=["egress"])
async def participant_recordings(
    session_id: str = Query(...),
    expires_in: int = Query(3600, description="Presigned URL TTL in seconds"),
):
    """
    Get presigned URLs for all per-participant OGG recordings.

    Each participant's audio is recorded separately with their identity in the filename.
    Returns 404 if no recordings exist yet (egress still running or no audio tracks published).
    """
    from models import ParticipantRecording, ParticipantRecordingsResponse

    prefix = f"TEMP/sessions/{session_id}/audio/"

    def _list_ogg_files():
        try:
            resp = s3_client.list_objects_v2(Bucket=settings.s3_bucket, Prefix=prefix)
        except BotoClientError as e:
            raise HTTPException(status_code=500, detail=f"S3 error: {e}")

        contents = resp.get("Contents", [])
        ogg_files = [obj for obj in contents if obj["Key"].endswith(".ogg")]

        results = []
        for obj in ogg_files:
            s3_key = obj["Key"]
            identity = s3_key.split("/")[-1].removesuffix(".ogg")
            try:
                url = s3_client.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": settings.s3_bucket, "Key": s3_key},
                    ExpiresIn=expires_in,
                )
                results.append({"identity": identity, "s3_key": s3_key, "url": url})
            except BotoClientError:
                continue
        return results

    try:
        file_list = await asyncio.to_thread(_list_ogg_files)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing recordings: {e}")

    if not file_list:
        raise HTTPException(
            status_code=404,
            detail=f"No per-participant recordings found for session {session_id}. "
                   "Egress may still be running or no audio tracks detected.",
        )

    # Map identity → display name from live room participants (best-effort; room may be gone)
    identity_to_role: dict[str, str] = {}
    try:
        participants = await get_room_participants(session_id)
        identity_to_role = {p["identity"]: p.get("name", p["identity"]) for p in participants}
    except Exception:
        pass

    recordings = [
        ParticipantRecording(
            identity=f["identity"],
            role=identity_to_role.get(f["identity"], f["identity"]),
            s3_key=f["s3_key"],
            url=f["url"],
            expires_in=expires_in,
        )
        for f in file_list
    ]

    return ParticipantRecordingsResponse(
        session_id=session_id,
        recordings=recordings,
        expires_in=expires_in,
    )


@app.get("/egress/list", tags=["egress"])
async def egress_list(session_id: str = Query(...)):
    """List all egress jobs for a session (active + historical)."""
    try:
        return await list_egress(room_name=session_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Participants ──────────────────────────────────────────────────────────────

@app.get("/room/participants", response_model=ParticipantsResponse, tags=["room"])
async def room_participants(session_id: str = Query(...)):
    """
    List participants currently in the room.
    Use this to confirm all 3 roles have joined before starting egress.
    """
    try:
        participants = await get_room_participants(room_name=session_id)
        return ParticipantsResponse(
            session_id=session_id,
            count=len(participants),
            participants=participants,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
