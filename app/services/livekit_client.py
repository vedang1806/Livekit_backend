"""
app/services/livekit_client.py — LiveKit Twirp API calls via httpx.

Manages:
- A persistent AsyncClient (initialized in app lifespan via init_http_client).
- A JWT token cache (9-min TTL) to avoid HMAC signing on every request.
- All room and egress operations against the LiveKit Twirp HTTP API.
"""

import time
import httpx

from app.config import settings
from app.services.tokens import (
    generate_admin_token,
    generate_egress_token,
    generate_room_admin_token,
)


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


# ── JWT token cache (9-min TTL, tokens expire in 10 min) ─────────────────────

_token_cache: dict[str, tuple[str, float]] = {}


def _cached_token(cache_key: str, generator) -> str:
    now   = time.monotonic()
    entry = _token_cache.get(cache_key)
    if entry:
        token, exp = entry
        if now < exp:
            return token
    token = generator()
    _token_cache[cache_key] = (token, now + 540)
    return token


# ── Header builders ───────────────────────────────────────────────────────────

def _http_base() -> str:
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
    client = _get_client()
    resp   = await client.post(url, json=body, headers=headers)
    if resp.status_code not in (200, 201):
        print(f"  LiveKit error {resp.status_code} → {url}\n  Response: {resp.text}")
    resp.raise_for_status()
    return resp.json()


# ── Room ──────────────────────────────────────────────────────────────────────

async def create_room(session_id: str) -> dict:
    url  = f"{_http_base()}/twirp/livekit.RoomService/CreateRoom"
    body = {
        "name":             session_id,
        "empty_timeout":    300,
        "max_participants": 10,
        "metadata":         f'{{"session_id":"{session_id}","hipaa_mode":true}}',
    }
    return await _post(url, body, _admin_headers())


async def get_room_participants(room_name: str) -> list:
    url    = f"{_http_base()}/twirp/livekit.RoomService/ListParticipants"
    body   = {"room": room_name}
    client = _get_client()
    resp   = await client.post(url, json=body, headers=_room_admin_headers(room_name))
    if resp.status_code not in (200, 201):
        print(f"  ListParticipants error {resp.status_code}: {resp.text}")
    resp.raise_for_status()
    return [
        {
            "identity":  p.get("identity", ""),
            "name":      p.get("name", ""),
            "state":     p.get("state", ""),
            "joined_at": p.get("joined_at", ""),
            "tracks":    len(p.get("tracks", [])),
        }
        for p in resp.json().get("participants", [])
    ]


# ── Egress ────────────────────────────────────────────────────────────────────

async def start_composite_egress(
    room_name: str, session_id: str, audio_only: bool = False
) -> dict:
    """Start a RoomCompositeEgress using grid-dark layout → S3 as MP4 or OGG."""
    file_type = "OGG" if audio_only else "MP4"
    extension = "ogg" if audio_only else "mp4"
    s3_key    = f"TEMP/sessions/{session_id}/composite_recording.{extension}"
    s3_url    = f"https://{settings.s3_bucket}.s3.{settings.aws_region}.amazonaws.com/{s3_key}"

    body = {
        "room_name":    room_name,
        "audio_only":   audio_only,
        "layout":       "grid-dark",
        "file_outputs": [{
            "file_type": file_type,
            "filepath":  s3_key,
            "s3": {
                "access_key": settings.aws_access_key,
                "secret":     settings.aws_secret_key,
                "region":     settings.aws_region,
                "bucket":     settings.s3_bucket,
                "key":        s3_key,
            },
        }],
    }
    data = await _post(
        f"{_http_base()}/twirp/livekit.Egress/StartRoomCompositeEgress",
        body,
        _egress_headers(room_name),
    )
    data["s3_key"] = s3_key
    data["s3_url"] = s3_url
    print(f"  Composite started: {data.get('egressId', '?')} | {s3_url}")
    return data


async def stop_egress(egress_id: str, room_name: str) -> dict:
    """
    Stop a running egress.
    412 = already ended — treated as success.
    """
    client = _get_client()
    resp   = await client.post(
        f"{_http_base()}/twirp/livekit.Egress/StopEgress",
        json={"egress_id": egress_id},
        headers=_egress_headers(room_name),
    )
    if resp.status_code == 412:
        print(f"  Egress {egress_id} already stopped — OK.")
        return {"egress_id": egress_id, "status": "ENDED"}
    if resp.status_code not in (200, 201):
        print(f"  StopEgress error {resp.status_code}: {resp.text}")
    resp.raise_for_status()
    return resp.json()


async def start_track_egress(
    room_name: str,
    session_id: str,
    track_sid: str,
    identity: str,
    track_kind: str = "audio",
) -> dict:
    """Record a single participant track. audio → OGG, video → WebM."""
    s3_key = (
        f"TEMP/sessions/{session_id}/video/{identity}.webm"
        if track_kind == "video"
        else f"TEMP/sessions/{session_id}/audio/{identity}.ogg"
    )
    s3_url = f"https://{settings.s3_bucket}.s3.{settings.aws_region}.amazonaws.com/{s3_key}"
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
    data = await _post(
        f"{_http_base()}/twirp/livekit.Egress/StartTrackEgress",
        body,
        _egress_headers(room_name),
    )
    data["s3_key"] = s3_key
    data["s3_url"] = s3_url
    print(f"  Track egress started: {identity} [{track_kind}] → {s3_url}")
    return data


async def list_egress(room_name: str) -> dict:
    client = _get_client()
    resp   = await client.post(
        f"{_http_base()}/twirp/livekit.Egress/ListEgress",
        json={"room_name": room_name},
        headers=_egress_headers(room_name),
    )
    if resp.status_code not in (200, 201):
        print(f"  ListEgress error {resp.status_code}: {resp.text}")
    resp.raise_for_status()
    return resp.json()
