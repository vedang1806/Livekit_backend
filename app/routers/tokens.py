from fastapi import APIRouter, HTTPException, Query

from app.config import settings
from app.models import TokenResponse
from app.services.tokens import generate_participant_token

router = APIRouter(tags=["Token"])

_VALID_ROLES = {"patient", "doctor", "interpreter"}


@router.get("/token", response_model=TokenResponse)
async def get_token(
    room:     str = Query(..., description="LiveKit room / session_id"),
    identity: str = Query(..., description="Unique participant identity string"),
    role:     str = Query(..., description="patient | doctor | interpreter"),
):
    """Mint a participant JWT for the LiveKit frontend SDK."""
    if role not in _VALID_ROLES:
        raise HTTPException(status_code=400, detail=f"role must be one of {_VALID_ROLES}")

    token = generate_participant_token(
        room_name=room,
        identity=identity,
        display_name=role.upper(),
        api_key=settings.livekit_api_key,
        api_secret=settings.livekit_api_secret,
    )
    return TokenResponse(token=token, url=settings.livekit_url, identity=identity, role=role, room=room)
