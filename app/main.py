"""
app/main.py — FastAPI application factory.

Responsibilities:
  - App lifecycle (lifespan): initialize HTTP client, start GC task, shut down cleanly.
  - Middleware: GZip compression, CORS, session cookie (for admin).
  - Admin panel mounted at /admin.
  - Mount all routers.

No business logic lives here.
"""

import asyncio
import logging
import uvicorn
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from sqlalchemy import text
from sqladmin import Admin
from starlette.middleware.sessions import SessionMiddleware

from app.admin.auth import AdminAuth
from app.admin.views import (
    ComplianceReportAdmin,
    EgressJobAdmin,
    ParticipantAdmin,
    RecordingAdmin,
    SessionAdmin,
    WebhookEventAdmin,
)
from app.config import settings
from app.db.session import engine
from app.services.livekit_client import init_http_client, close_http_client
from app.state.session import state
from app.routers import rooms, tokens, egress, events, webhook

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Lightweight DB connectivity check on startup
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        logging.info("  DB      : connected")
    except Exception as e:
        logging.warning(f"  DB      : unreachable — {e}")

    await init_http_client()
    gc_task = asyncio.create_task(state.run_gc())

    logging.info("Interpreter Backend starting")
    logging.info(f"  LiveKit : {settings.livekit_url}")
    logging.info(f"  S3      : s3://{settings.s3_bucket} ({settings.aws_region})")
    logging.info(f"  Admin   : http://<host>/admin")

    yield

    gc_task.cancel()
    try:
        await gc_task
    except asyncio.CancelledError:
        pass
    await close_http_client()
    await engine.dispose()
    logging.info("Interpreter Backend shutting down.")


app = FastAPI(
    title="Interpreter Backend",
    version="1.0.0",
    lifespan=lifespan,
    root_path="/api",
    # It causes sqladmin static assets and redirects to break when running directly.
    docs_url="/docs",
    redoc_url="/redoc",
)

# Session cookie required by sqladmin auth
app.add_middleware(SessionMiddleware, secret_key=settings.admin_secret)
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Admin panel — available at /admin ─────────────────────────────────────────
admin = Admin(app, engine, authentication_backend=AdminAuth(secret_key=settings.admin_secret))
admin.add_view(SessionAdmin)
admin.add_view(ParticipantAdmin)
admin.add_view(EgressJobAdmin)
admin.add_view(RecordingAdmin)
admin.add_view(ComplianceReportAdmin)
admin.add_view(WebhookEventAdmin)

# ── Health check ──────────────────────────────────────────────────────────────
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

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(tokens.router)
app.include_router(rooms.router)
app.include_router(egress.router)
app.include_router(events.router)
app.include_router(webhook.router)


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
