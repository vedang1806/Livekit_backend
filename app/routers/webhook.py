from fastapi import APIRouter, Request

from app.webhooks.handlers import handle_livekit_webhook

router = APIRouter(tags=["System"])


@router.post("/webhook/livekit")
async def livekit_webhook(request: Request):
    """
    Receives LiveKit server-side events (room lifecycle, egress state changes).
    Configure this URL in your LiveKit Cloud dashboard → Webhooks.
    """
    return await handle_livekit_webhook(request)
