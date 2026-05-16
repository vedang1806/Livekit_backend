"""
interpreter-backend — main.py
FastAPI entry point: token minting, room management, health check.
"""

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse
from contextlib import asynccontextmanager
import asyncio
import json
import logging
import uvicorn

logging.basicConfig(level=logging.INFO)

from config import settings
from tokens import generate_participant_token, generate_admin_token
from webhook import handle_livekit_webhook, clear_active_egress, subscribe_sse, unsubscribe_sse
from egress import (
    create_room,
    start_composite_egress,
    stop_egress,
    list_egress,
    get_room_participants,
    get_recording_presigned_url,
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
    HealthResponse,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"✅ Interpreter Backend starting")
    print(f"   LiveKit : {settings.livekit_url}")
    print(f"   S3      : s3://{settings.s3_bucket} ({settings.aws_region})")
    yield
    print("Interpreter Backend shutting down.")


app = FastAPI(
    title="Interpreter Backend",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")


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
    room: str  = Query(..., description="LiveKit room / session_id"),
    identity: str = Query(..., description="Unique participant identity string"),
    role: str  = Query(..., description="patient | doctor | interpreter"),
):
    """
    Mint a participant JWT for the LiveKit frontend SDK.
    The role is embedded as the display name so LiveKit renders it on the tile.
    """
    VALID_ROLES = {"patient", "doctor", "interpreter"}
    if role not in VALID_ROLES:
        raise HTTPException(status_code=400, detail=f"role must be one of {VALID_ROLES}")

    display_name = role.upper()  # short role label renders clearly on video tile
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
    import boto3
    from botocore.exceptions import ClientError as BotoClientError

    # ── 1. Check LiveKit egress state ─────────────────────────────────────────
    livekit_status = "unknown"
    egress_id      = None
    try:
        data    = await list_egress(room_name=session_id)
        items   = data.get("items", [])
        if items:
            latest       = items[-1]
            livekit_status = latest.get("status", "unknown")
            egress_id      = latest.get("egressId") or latest.get("egress_id")
        else:
            livekit_status = "no_egress_found"
    except Exception as e:
        livekit_status = f"error: {e}"

    # ── 2. Check S3 file ──────────────────────────────────────────────────────
    s3_key  = f"sessions/{session_id}/composite_recording.mp4"
    s3      = boto3.client(
        "s3",
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key,
        aws_secret_access_key=settings.aws_secret_key,
    )
    s3_ready = False
    size_mb  = None
    try:
        head    = s3.head_object(Bucket=settings.s3_bucket, Key=s3_key)
        size_mb = round(head["ContentLength"] / (1024 * 1024), 2)
        s3_ready = True
    except BotoClientError:
        pass

    status = "ready" if s3_ready else (
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
