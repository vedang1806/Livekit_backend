# Interpreter Backend

FastAPI backend for the AI Medical Interpreter Platform — Layer 1.
Handles LiveKit room creation, participant token minting, and egress management.

## Setup

```bash
cp .env.example .env
# Fill in .env with your LiveKit and AWS credentials

pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Service health + config check |
| POST | `/room/create` | Create a LiveKit room |
| GET | `/token` | Mint a participant JWT |
| GET | `/room/participants` | List participants in a room |
| POST | `/egress/start` | Start composite video egress → S3 |
| POST | `/egress/stop` | Stop a running egress |
| GET | `/egress/list` | List egress jobs for a session |

Interactive docs: http://localhost:8000/docs

## Session flow

```
1. POST /room/create        { session_id }
2. GET  /token × 3         ?room=&identity=&role=  → hand tokens to frontend
3. Frontend: all 3 join and publish audio/video
4. GET  /room/participants  → confirm count == 3
5. POST /egress/start       { session_id }          → recording starts
6. Session runs ...
7. POST /egress/stop        { egress_id, session_id }
8. MP4 lands in S3: sessions/<session_id>/composite_recording.mp4
```
