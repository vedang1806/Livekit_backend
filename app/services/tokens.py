"""
app/services/tokens.py — LiveKit JWT generation.
"""

import time
import jwt as pyjwt


def generate_admin_token(api_key: str, api_secret: str, ttl_seconds: int = 600) -> str:
    """Admin token for RoomService calls (CreateRoom, ListRooms, etc.)."""
    now = int(time.time())
    payload = {
        "iss": api_key, "sub": "server-admin",
        "iat": now, "exp": now + ttl_seconds, "nbf": now,
        "video": {"roomCreate": True, "roomAdmin": True, "roomList": True},
    }
    token = pyjwt.encode(payload, api_secret, algorithm="HS256")
    return token if isinstance(token, str) else token.decode("utf-8")


def generate_room_admin_token(
    room_name: str, api_key: str, api_secret: str, ttl_seconds: int = 600
) -> str:
    """
    Room-scoped admin token for ListParticipants etc.
    LiveKit Cloud rejects a global admin token for these endpoints.
    """
    now = int(time.time())
    payload = {
        "iss": api_key, "sub": "server-admin",
        "iat": now, "exp": now + ttl_seconds, "nbf": now,
        "video": {"roomAdmin": True, "roomList": True, "room": room_name},
    }
    token = pyjwt.encode(payload, api_secret, algorithm="HS256")
    return token if isinstance(token, str) else token.decode("utf-8")


def generate_egress_token(
    room_name: str, api_key: str, api_secret: str, ttl_seconds: int = 600
) -> str:
    """
    Egress token for EgressService calls.
    roomRecord: True is required — roomAdmin alone is rejected by LiveKit Cloud's egress daemon.
    """
    now = int(time.time())
    payload = {
        "iss": api_key, "sub": "server-egress",
        "iat": now, "exp": now + ttl_seconds, "nbf": now,
        "video": {
            "roomCreate": True, "roomAdmin": True,
            "roomRecord": True, "room": room_name,
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
    """Participant token for the frontend LiveKit SDK."""
    now = int(time.time())
    payload = {
        "iss": api_key, "sub": identity,
        "iat": now, "exp": now + ttl_seconds, "nbf": now,
        "name": display_name,
        "video": {
            "roomJoin": True, "room": room_name,
            "canPublish": can_publish, "canSubscribe": can_subscribe,
            "canPublishData": True,
        },
    }
    token = pyjwt.encode(payload, api_secret, algorithm="HS256")
    return token if isinstance(token, str) else token.decode("utf-8")
