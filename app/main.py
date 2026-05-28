"""
app/main.py — FastAPI application factory.

Responsibilities:
  - App lifecycle (lifespan): initialize HTTP client, start GC task, shut down cleanly.
  - Middleware: GZip compression, CORS.
  - Mount all routers.

No business logic lives here.
"""

import asyncio
import logging
import uvicorn
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import text
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from app.config import settings
from app.db.session import engine
from app.db.base import Base
from app.services.livekit_client import init_http_client, close_http_client
from app.state.session import state
from app.routers import rooms, tokens, egress, events, webhook

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Verify DB connectivity on startup (migrations must already be applied via `migrate` service)
    async with engine.begin() as conn:
        await conn.run_sync(lambda _: None)  # lightweight ping

    await init_http_client()
    gc_task = asyncio.create_task(state.run_gc())

    logging.info("Interpreter Backend starting")
    logging.info(f"  LiveKit : {settings.livekit_url}")
    logging.info(f"  S3      : s3://{settings.s3_bucket} ({settings.aws_region})")
    logging.info(f"  DB      : {settings.database_url.split('@')[-1]}")  # log host/db, not credentials

    yield

    gc_task.cancel()
    try:
        await gc_task
    except asyncio.CancelledError:
        pass
    await close_http_client()
    await engine.dispose()
    logging.info("Interpreter Backend shutting down.")

docs_url  ="/docs"
redoc_url ="/redoc"

app = FastAPI(
    title="Interpreter Backend",
    version="1.0.0",
    lifespan=lifespan,
    root_path="/api",
    docs_url=docs_url,
    redoc_url=redoc_url
)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

#health check endpoint for load balancer
@app.get("/health", tags=["System"])
async def health():
    db_ok = False
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        db_ok = True
    except Exception:
        pass

    return {
        "status":      "ok" if db_ok else "degraded",
        "livekit_url": settings.livekit_url,
        "s3_bucket":   settings.s3_bucket,
        "database":    "ok" if db_ok else "unreachable",
    }
#---- Mount routers --------------------------------------------------------------------------------
app.include_router(tokens.router)
app.include_router(rooms.router)
app.include_router(egress.router)
app.include_router(events.router)
app.include_router(webhook.router)


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
