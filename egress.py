"""
egress.py — LiveKit room + egress operations via Twirp HTTP API.

All functions are async and use httpx.
Errors print the raw LiveKit response before raising so debugging is easy.
"""

import httpx
import boto3
from botocore.exceptions import ClientError
from config import settings
from tokens import generate_admin_token, generate_egress_token, generate_room_admin_token


# ── Helpers ───────────────────────────────────────────────────────────────────

def _http_base() -> str:
    """Convert wss:// or ws:// LiveKit URL to https:// for REST calls."""
    return (
        settings.livekit_url
        .replace("wss://", "https://")
        .replace("ws://", "http://")
        .rstrip("/")
    )


def _admin_headers() -> dict:
    token = generate_admin_token(settings.livekit_api_key, settings.livekit_api_secret)
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }


def _egress_headers(room_name: str) -> dict:
    token = generate_egress_token(room_name, settings.livekit_api_key, settings.livekit_api_secret)
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }


async def _post(url: str, body: dict, headers: dict) -> dict:
    """POST helper with error logging."""
    async with httpx.AsyncClient(timeout=15) as client:
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
    room_headers = {
        "Authorization": f"Bearer {generate_room_admin_token(room_name, settings.livekit_api_key, settings.livekit_api_secret)}",
        "Content-Type":  "application/json",
    }
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, json=body, headers=room_headers)
        if resp.status_code not in (200, 201):
            print(f"  ListParticipants error {resp.status_code}: {resp.text}")
        resp.raise_for_status()
        data = resp.json()

    participants = data.get("participants", [])
    return [
        {
            "identity": p.get("identity", ""),
            "name":     p.get("name", ""),
            "state":    p.get("state", ""),
            "joined_at": p.get("joined_at", ""),
            "tracks":   len(p.get("tracks", [])),
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
    Start a RoomCompositeEgress that records the full room.

    Layout 'grid':
      - All participants tiled in a grid
      - Display name (set in JWT 'name' field) overlaid on each tile
      - Renders as MP4 (video+audio) or OGG (audio-only)

    Important: call this AFTER participants have joined and published tracks.
    LiveKit will abort the egress if no tracks are active when it starts.

    Returns dict with egress_id and s3_key.
    """
    file_type = "OGG" if audio_only else "MP4"
    extension = "ogg" if audio_only else "mp4"
    s3_key    = f"sessions/{session_id}/composite_recording.{extension}"

    url  = f"{_http_base()}/twirp/livekit.Egress/StartRoomCompositeEgress"
    body = {
        "room_name":  room_name,
        "layout":     "grid-dark",   # dark theme — white name labels clearly visible per tile
        "audio_only": audio_only,
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

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, json=body, headers=_egress_headers(room_name))

        # 412 = already ended/aborted — not an error
        if resp.status_code == 412:
            print(f"  Egress {egress_id} already stopped (aborted/ended) — OK.")
            return {"egress_id": egress_id, "status": "ENDED"}

        if resp.status_code not in (200, 201):
            print(f"  StopEgress error {resp.status_code}: {resp.text}")
        resp.raise_for_status()
        data = resp.json()

        # Print the S3 destination from the egress response if available
        for output in data.get("file_results", []):
            location = output.get("location", "")
            if location:
                print(f"  Recording saved : {location}")
            else:
                # Fallback: reconstruct from filename if location absent
                fname = output.get("filename", "")
                if fname:
                    print(f"  Recording file  : s3://{settings.s3_bucket}/{fname}")

        return data


async def start_track_egress(
    room_name: str,
    session_id: str,
    track_sid: str,
    identity: str,
) -> dict:
    """
    Record a single participant's audio track as OGG to S3.
    Called automatically on track_published (AUDIO) webhook events.
    S3 path: sessions/{session_id}/audio/{identity}.ogg
    """
    s3_key = f"sessions/{session_id}/audio/{identity}.ogg"
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
    print(f"  Track egress started : {egress_id} | {identity} → {s3_url}")
    data["s3_key"] = s3_key
    data["s3_url"] = s3_url
    return data


def get_recording_presigned_url(session_id: str, expires_in: int = 3600) -> dict:
    """
    Generate a presigned URL for the session's composite recording.
    Raises FileNotFoundError if the file hasn't been uploaded to S3 yet
    (egress still running or upload in progress).
    """
    s3_key = f"sessions/{session_id}/composite_recording.mp4"
    s3_client = boto3.client(
        "s3",
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key,
        aws_secret_access_key=settings.aws_secret_key,
    )

    # Verify the file exists before handing out a URL that will 404.
    try:
        s3_client.head_object(Bucket=settings.s3_bucket, Key=s3_key)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            raise FileNotFoundError(
                f"Recording not ready yet for session '{session_id}'. "
                "Wait until the session ends and the upload completes (~10–30s after stop)."
            )
        raise

    url = s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": settings.s3_bucket, "Key": s3_key},
        ExpiresIn=expires_in,
    )
    print(f"  Presigned URL ({expires_in}s): {url}")
    return {"s3_key": s3_key, "url": url, "expires_in": expires_in}


async def list_egress(room_name: str) -> dict:
    """
    List all egress jobs for a room (active + historical).
    Useful for checking recording status from the frontend.
    """
    url  = f"{_http_base()}/twirp/livekit.Egress/ListEgress"
    body = {"room_name": room_name}

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, json=body, headers=_egress_headers(room_name))
        if resp.status_code not in (200, 201):
            print(f"  ListEgress error {resp.status_code}: {resp.text}")
        resp.raise_for_status()
        return resp.json()
