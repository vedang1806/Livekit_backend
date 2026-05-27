"""
app/models.py — Pydantic request and response models for all endpoints.
"""

from typing import List, Optional
from pydantic import BaseModel


# ── Room ──────────────────────────────────────────────────────────────────────

class CreateRoomRequest(BaseModel):
    session_id: str

class CreateRoomResponse(BaseModel):
    session_id:  str
    room_sid:    str
    livekit_url: str


# ── Token ─────────────────────────────────────────────────────────────────────

class TokenResponse(BaseModel):
    token:    str
    url:      str
    identity: str
    role:     str
    room:     str


# ── Egress ────────────────────────────────────────────────────────────────────

class StartEgressRequest(BaseModel):
    session_id: str
    audio_only: bool = False    # False = MP4 video+audio, True = OGG audio-only

class StartEgressResponse(BaseModel):
    egress_id:  str
    session_id: str
    s3_key:     str
    status:     str

class StopEgressRequest(BaseModel):
    egress_id:  str
    session_id: str             # needed to mint egress token with correct room scope

class StopEgressResponse(BaseModel):
    egress_id: str
    status:    str


# ── Participants ──────────────────────────────────────────────────────────────

class ParticipantInfo(BaseModel):
    identity:  str
    name:      str
    state:     str
    joined_at: str
    tracks:    int

class ParticipantsResponse(BaseModel):
    session_id:   str
    count:        int
    participants: List[ParticipantInfo]


# ── Recording URLs ────────────────────────────────────────────────────────────

class RecordingUrlResponse(BaseModel):
    session_id: str
    s3_key:     str
    url:        str
    expires_in: int

class ParticipantRecording(BaseModel):
    identity:   str
    role:       str
    s3_key:     str
    url:        str
    expires_in: int

class ParticipantRecordingsResponse(BaseModel):
    session_id: str
    recordings: List[ParticipantRecording]
    expires_in: int

class SessionRecording(BaseModel):
    kind:       str   # "composite" | "audio" | "video"
    identity:   str   # empty string for composite
    s3_key:     str
    url:        str
    expires_in: int

class SessionRecordingsResponse(BaseModel):
    session_id: str
    composite:  Optional[SessionRecording]
    audio:      List[SessionRecording]
    video:      List[SessionRecording]
    expires_in: int

class HealthResponse(BaseModel):
    status:      str
    livekit_url: str
    s3_bucket:   str
