from fastapi import APIRouter, HTTPException, Query

from app.config import settings
from app.models import CreateRoomRequest, CreateRoomResponse, ParticipantsResponse
from app.services.livekit_client import create_room, get_room_participants

router = APIRouter(tags=["Room"])


@router.post("/room/create", response_model=CreateRoomResponse)
async def room_create(body: CreateRoomRequest):
    """Create a LiveKit room for a new interpreter session."""
    try:
        room = await create_room(body.session_id)
        return CreateRoomResponse(
            session_id=body.session_id,
            room_sid=room.get("sid", ""),
            livekit_url=settings.livekit_url,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/room/participants", response_model=ParticipantsResponse)
async def room_participants(session_id: str = Query(...)):
    """List participants currently in the room."""
    try:
        participants = await get_room_participants(room_name=session_id)
        return ParticipantsResponse(
            session_id=session_id,
            count=len(participants),
            participants=participants,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
