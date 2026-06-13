"""Application entrypoint — REST core under /api, live stream, dashboard.

Two liveness modes:
  * persistent server (uvicorn/Docker): an asyncio background ticker advances
    the simulation every ~0.7s and broadcasts to /ws/live subscribers;
  * serverless (env VERCEL set): no background task — app/realtime.py advances
    the simulation lazily from elapsed wall time on each REST call.
"""
import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from . import realtime
from .core import router as core_router

TICK_INTERVAL = 0.7


async def _ticker():
    while True:
        events = realtime.tick(1)
        await realtime.broadcast(events)
        await asyncio.sleep(TICK_INTERVAL)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    task = None
    if not os.environ.get("VERCEL"):     # serverless uses lazy ticking instead
        task = asyncio.create_task(_ticker())
    try:
        yield
    finally:
        if task:
            task.cancel()


app = FastAPI(title="VITALS — Live Environmental Early-Warning", version="0.1.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"],
                   allow_headers=["*"])
app.include_router(core_router, prefix="/api")
app.include_router(realtime.router)      # /api/live/* + /ws/live

FRONTEND = Path(__file__).resolve().parents[2] / "frontend" / "index.html"


@app.get("/health")
def health():
    return {"status": "ok", "service": "vitals-triage-assistant"}


@app.get("/")
def index():
    if FRONTEND.exists():
        return FileResponse(FRONTEND)
    return {"service": "vitals-triage-assistant", "docs": "/docs"}
