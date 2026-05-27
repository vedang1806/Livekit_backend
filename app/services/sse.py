"""
Thin facade over state.session so routers never touch state directly.
To move SSE from in-process queues to Redis pub/sub, only rewrite this file.
"""

import asyncio
from app.state.session import state


def subscribe(session_id: str) -> asyncio.Queue:
    return state.subscribe_sse(session_id)


def unsubscribe(session_id: str, queue: asyncio.Queue) -> None:
    state.unsubscribe_sse(session_id, queue)
