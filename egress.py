"""
egress.py — LiveKit room + egress operations via Twirp HTTP API.

All functions are async and use a shared httpx.AsyncClient (initialized via
init_http_client() in the app lifespan). Tokens are cached for 9 minutes to
avoid HMAC-SHA256 signing on every request. boto3 S3 calls run in a thread
pool via asyncio.to_thread() so they never block the event loop.
"""

import asyncio
import time

import boto3
import httpx
from botocore.exceptions import ClientError

from config import settings
from tokens import generate_admin_token, generate_egress_token, generate_room_admin_token


# ── HTTP client (persistent connection pool) ──────────────────────────────────

_http_client: httpx.AsyncClient | None = None


async def init_http_client() -> None:
    global _http_client
    _http_client = httpx.AsyncClient(timeout=15)


async def close_http_client() -> None:
    global _http_client
    if _http_client and not _http_client.is_closed:
        await _http_client.aclose()
    _http_client = None


def _get_client() -> httpx.AsyncClient:
    if _http_client is None or _http_client.is_closed:
        raise RuntimeError("HTTP client not initialized — call init_http_client() in lifespan")
    return _http_client


# ── S3 client singleton (thread-safe, reuses connection pool) ─────────────────

s3_client = boto3.client(
    "s3",
    region_name=settings.aws_region,
    aws_access_key_id=settings.aws_access_key,
    aws_secret_access_key=settings.aws_secret_key,
)


# ── JWT token cache (9-min TTL, tokens expire in 10 min) ─────────────────────

_token_cache: dict[str, tuple[str, float]] = {}


def _cached_token(cache_key: str, generator) -> str:
    """Return a cached JWT or generate + cache a fresh one."""
    now = time.monotonic()
    entry = _token_cache.get(cache_key)
    if entry:
        token, exp = entry
        if now < exp:
            return token
    token = generator()
    _token_cache[cache_key] = (token, now + 540)  # cache for 9 min (token TTL = 10 min)
    return token


# ── Header builders (use cached tokens) ──────────────────────────────────────

def _http_base() -> str:
    """Convert wss:// or ws:// LiveKit URL to https:// for REST calls."""
    return (
        settings.livekit_url
        .replace("wss://", "https://")
        .replace("ws://", "http://")
        .rstrip("/")
    )


def _admin_headers() -> dict:
    token = _cached_token(
        "admin",
        lambda: generate_admin_token(settings.livekit_api_key, settings.livekit_api_secret),
    )
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _egress_headers(room_name: str) -> dict:
    token = _cached_token(
        f"egress:{room_name}",
        lambda: generate_egress_token(room_name, settings.livekit_api_key, settings.livekit_api_secret),
    )
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _room_admin_headers(room_name: str) -> dict:
    token = _cached_token(
        f"room_admin:{room_name}",
        lambda: generate_room_admin_token(room_name, settings.livekit_api_key, settings.livekit_api_secret),
    )
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


# ── HTTP helper ───────────────────────────────────────────────────────────────

async def _post(url: str, body: dict, headers: dict) -> dict:
    """POST to LiveKit Twirp API using the shared persistent client."""
    client = _get_client()
    resp = await client.post(url, json=body, headers=headers)
    if resp.status_code not in (200, 201):
        print(f"  LiveKit error {resp.status_code} → {url}")
        print(f"  Response: {resp.text}")
    resp.raise_for_status()
    return resp.json()


# ── Room ──────────────────────────────────────────────────────────────────────

async def create_room(session_id: str) -> dict:
    """
    Create a LiveKit room for a session.
    room_name = session_id (1-to-1 mapping).
    """
    url  = f"{_http_base()}/twirp/livekit.RoomService/CreateRoom"
    body = {
        "name":             session_id,
        "empty_timeout":    300,    # auto-destroy after 5 min empty
        "max_participants": 10,
        "metadata":         f'{{"session_id":"{session_id}","hipaa_mode":true}}',
    }
    return await _post(url, body, _admin_headers())


async def get_room_participants(room_name: str) -> list:
    """
    List participants currently in the room.
    Returns a list of participant dicts with identity, name, state.
    """
    url  = f"{_http_base()}/twirp/livekit.RoomService/ListParticipants"
    body = {"room": room_name}

    # ListParticipants requires a room-scoped token on LiveKit Cloud;
    # a global admin token (no 'room' field) returns 401.
    client = _get_client()
    resp = await client.post(url, json=body, headers=_room_admin_headers(room_name))
    if resp.status_code not in (200, 201):
        print(f"  ListParticipants error {resp.status_code}: {resp.text}")
    resp.raise_for_status()
    data = resp.json()

    participants = data.get("participants", [])
    return [
        {
            "identity":  p.get("identity", ""),
            "name":      p.get("name", ""),
            "state":     p.get("state", ""),
            "joined_at": p.get("joined_at", ""),
            "tracks":    len(p.get("tracks", [])),
        }
        for p in participants
    ]


# ── Egress ────────────────────────────────────────────────────────────────────

async def start_composite_egress(
    room_name: str,
    session_id: str,
    audio_only: bool = False,
) -> dict:
    """
    Start a RoomCompositeEgress that records the full room using the built-in grid-dark layout.
    Renders as MP4 (video+audio) or OGG (audio-only).

    Important: call this AFTER participants have joined and published tracks.
    LiveKit will abort the egress if no tracks are active when it starts.
    """
    file_type = "OGG" if audio_only else "MP4"
    extension = "ogg" if audio_only else "mp4"
    s3_key    = f"TEMP/sessions/{session_id}/composite_recording.{extension}"

    url = f"{_http_base()}/twirp/livekit.Egress/StartRoomCompositeEgress"

    body: dict = {
        "room_name":  room_name,
        "audio_only": audio_only,
        "layout":     "grid-dark",
    }

    body["file_outputs"] = [{
        "file_type": file_type,
        "filepath":  s3_key,
        "s3": {
            "access_key": settings.aws_access_key,
            "secret":     settings.aws_secret_key,
            "region":     settings.aws_region,
            "bucket":     settings.s3_bucket,
            "key":        s3_key,
        },
    }]

    data = await _post(url, body, _egress_headers(room_name))
    data["s3_key"] = s3_key

    s3_url = f"https://{settings.s3_bucket}.s3.{settings.aws_region}.amazonaws.com/{s3_key}"
    print(f"  Egress started  : {data.get('egress_id', 'unknown')}")
    print(f"  Recording S3 URL: {s3_url}")

    data["s3_url"] = s3_url
    return data


async def stop_egress(egress_id: str, room_name: str) -> dict:
    """
    Stop a running egress job.

    412 (failed_precondition) means the egress already ended or was aborted
    by LiveKit — treated as success, not an error.
    """
    url  = f"{_http_base()}/twirp/livekit.Egress/StopEgress"
    body = {"egress_id": egress_id}

    client = _get_client()
    resp = await client.post(url, json=body, headers=_egress_headers(room_name))

    # 412 = already ended/aborted — not an error
    if resp.status_code == 412:
        print(f"  Egress {egress_id} already stopped (aborted/ended) — OK.")
        return {"egress_id": egress_id, "status": "ENDED"}

    if resp.status_code not in (200, 201):
        print(f"  StopEgress error {resp.status_code}: {resp.text}")
    resp.raise_for_status()
    data = resp.json()

    for output in data.get("file_results", []):
        location = output.get("location", "")
        if location:
            print(f"  Recording saved : {location}")
        else:
            fname = output.get("filename", "")
            if fname:
                print(f"  Recording file  : s3://{settings.s3_bucket}/{fname}")

    return data


async def start_track_egress(
    room_name: str,
    session_id: str,
    track_sid: str,
    identity: str,
    track_kind: str = "audio",   # "audio" → OGG, "video" → WebM
) -> dict:
    """
    Record a single participant track to S3.
      audio → TEMP/sessions/{session_id}/audio/{identity}.ogg
      video → TEMP/sessions/{session_id}/video/{identity}.webm
    """
    if track_kind == "video":
        s3_key = f"TEMP/sessions/{session_id}/video/{identity}.webm"
    else:
        s3_key = f"TEMP/sessions/{session_id}/audio/{identity}.ogg"
    url    = f"{_http_base()}/twirp/livekit.Egress/StartTrackEgress"
    body   = {
        "room_name": room_name,
        "track_id":  track_sid,
        "file": {
            "filepath": s3_key,
            "s3": {
                "access_key": settings.aws_access_key,
                "secret":     settings.aws_secret_key,
                "region":     settings.aws_region,
                "bucket":     settings.s3_bucket,
                "key":        s3_key,
            },
        },
    }
    data = await _post(url, body, _egress_headers(room_name))
    egress_id = data.get("egressId", data.get("egress_id", ""))
    s3_url = f"https://{settings.s3_bucket}.s3.{settings.aws_region}.amazonaws.com/{s3_key}"
    print(f"  Track egress started : {egress_id} | {identity} [{track_kind}] → {s3_url}")
    data["s3_key"] = s3_key
    data["s3_url"] = s3_url
    return data


async def get_recording_presigned_url(session_id: str, expires_in: int = 3600) -> dict:
    """
    Generate a presigned URL for the session's composite recording.
    Runs the boto3 calls in a thread pool to avoid blocking the event loop.
    Raises FileNotFoundError if the file hasn't landed in S3 yet.
    """
    def _blocking():
        s3_key = f"TEMP/sessions/{session_id}/composite_recording.mp4"
        try:
            s3_client.head_object(Bucket=settings.s3_bucket, Key=s3_key)
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("404", "NoSuchKey"):
                raise FileNotFoundError(
                    f"Recording not ready yet for session '{session_id}'. "
                    "Egress may still be running or file is uploading. "
                    "Retry after ~10–30s."
                )
            raise

        url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": settings.s3_bucket, "Key": s3_key},
            ExpiresIn=expires_in,
        )
        print(f"  Presigned URL ({expires_in}s): {url}")
        return {"s3_key": s3_key, "url": url, "expires_in": expires_in}

    return await asyncio.to_thread(_blocking)


async def list_egress(room_name: str) -> dict:
    """
    List all egress jobs for a room (active + historical).
    Useful for checking recording status from the frontend.
    """
    url  = f"{_http_base()}/twirp/livekit.Egress/ListEgress"
    body = {"room_name": room_name}

    client = _get_client()
    resp = await client.post(url, json=body, headers=_egress_headers(room_name))
    if resp.status_code not in (200, 201):
        print(f"  ListEgress error {resp.status_code}: {resp.text}")
    resp.raise_for_status()
    return resp.json()
