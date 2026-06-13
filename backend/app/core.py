"""REST surface for the VITALS environmental early-warning engine.

The same Engine singleton that drives the live stream also scores interactive
POST requests, so manually submitted readings join the shared rolling state.
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .engine import engine

router = APIRouter()


class StationReading(BaseModel):
    station: str
    temp: float
    feels: float
    humidity: float
    wind: float
    pressure: float
    source: str = "manual"


@router.post("/reading")
def submit_reading(reading: StationReading):
    """Submit a station environmental reading; returns risk score + triage decision.

    Response includes: score, triage (green/amber/red), severity (ok/warn/critical),
    trend (rising/stable/falling), per-signal contributions, and a human-readable summary.
    """
    result = engine.submit_reading(reading.model_dump())
    return result


@router.get("/station/{name}")
def get_station(name: str):
    """Latest scored state and trend for a specific station."""
    state = engine.station_state(name)
    if state is None:
        raise HTTPException(status_code=404, detail="Station not found")
    return state


@router.get("/alert-queue")
def get_alert_queue(limit: int = 50):
    """All monitored stations ranked by risk score (highest first)."""
    return {"queue": engine.alert_queue(limit=limit), "total": len(engine._latest)}
