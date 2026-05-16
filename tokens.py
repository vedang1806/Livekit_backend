"""
tokens.py — LiveKit JWT generation.

Two token types:
  generate_admin_token()       — server-side API calls (RoomService)
  generate_egress_token()      — EgressService calls (roomRecord grant required)
  generate_participant_token() — frontend SDK (roomJoin grant)
"""

import time
import jwt as pyjwt


def generate_admin_token(
    api_key: str,
    api_secret: str,
    ttl_seconds: int = 600,
) -> str:
    """
    Admin token for RoomService calls (CreateRoom, ListRooms, etc.).
    Grants: roomCreate + roomAdmin + roomList.
    No 'room' restriction — applies across all rooms.
    """
    now = int(time.time())
    payload = {
        "iss": api_key,
        "sub": "server-admin",
        "iat": now,
        "exp": now + ttl_seconds,
        "nbf": now,
        "video": {
            "roomCreate": True,
            "roomAdmin":  True,
            "roomList":   True,
        },
    }
    token = pyjwt.encode(payload, api_secret, algorithm="HS256")
    return token if isinstance(token, str) else token.decode("utf-8")


def generate_room_admin_token(
    room_name: str,
    api_key: str,
    api_secret: str,
    ttl_seconds: int = 600,
) -> str:
    """
    Room-scoped admin token for RoomService calls that require a specific room
    (e.g. ListParticipants, MutePublishedTrack). LiveKit Cloud rejects a global
    admin token (no 'room' field) for these endpoints — must be scoped.
    """
    now = int(time.time())
    payload = {
        "iss": api_key,
        "sub": "server-admin",
        "iat": now,
        "exp": now + ttl_seconds,
        "nbf": now,
        "video": {
            "roomAdmin": True,
            "roomList":  True,
            "room":      room_name,
        },
    }
    token = pyjwt.encode(payload, api_secret, algorithm="HS256")
    return token if isinstance(token, str) else token.decode("utf-8")


def generate_egress_token(
    room_name: str,
    api_key: str,
    api_secret: str,
    ttl_seconds: int = 600,
) -> str:
    """
    Egress token for EgressService calls (start/stop egress).
    Requires roomRecord: True — roomAdmin alone is rejected by LiveKit Cloud's
    egress daemon (it validates grants independently from RoomService).
    Must be scoped to the target room_name.
    """
    now = int(time.time())
    payload = {
        "iss": api_key,
        "sub": "server-egress",
        "iat": now,
        "exp": now + ttl_seconds,
        "nbf": now,
        "video": {
            "roomCreate": True,
            "roomAdmin":  True,
            "roomRecord": True,   # required by EgressService
            "room":       room_name,
        },
    }
    token = pyjwt.encode(payload, api_secret, algorithm="HS256")
    return token if isinstance(token, str) else token.decode("utf-8")


def generate_participant_token(
    room_name: str,
    identity: str,
    display_name: str,
    api_key: str,
    api_secret: str,
    can_publish: bool = True,
    can_subscribe: bool = True,
    ttl_seconds: int = 3600,
) -> str:
    """
    Participant token for the frontend LiveKit SDK.
    The 'name' field is rendered as the display name overlay on the video tile.
    Grant: roomJoin scoped to room_name.
    """
    now = int(time.time())
    payload = {
        "iss": api_key,
        "sub": identity,
        "iat": now,
        "exp": now + ttl_seconds,
        "nbf": now,
        "name": display_name,       # shown on tile in LiveKit UI
        "video": {
            "roomJoin":       True,
            "room":           room_name,
            "canPublish":     can_publish,
            "canSubscribe":   can_subscribe,
            "canPublishData": True,
        },
    }
    token = pyjwt.encode(payload, api_secret, algorithm="HS256")
    return token if isinstance(token, str) else token.decode("utf-8")
