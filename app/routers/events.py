import asyncio
import json

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse

from app.services.sse import subscribe, unsubscribe

router = APIRouter(tags=["Egress"])


@router.get("/egress/events")
async def egress_events(session_id: str = Query(...)):
    """
    SSE stream — fires once with 'egress_ended' when recording completes.
    Frontend subscribes after session start and waits for the event,
    then calls /egress/recording-url to get the presigned download link.
    """
    queue = subscribe(session_id)

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
            unsubscribe(session_id, queue)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
