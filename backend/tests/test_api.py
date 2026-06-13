"""Smoke tests generated from the blueprint contract."""
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)

SMOKE = [
  {
    "method": "post",
    "path": "/api/reading",
    "json": {
      "station": "TestCity",
      "temp": 42.0,
      "feels": 46.0,
      "humidity": 88,
      "wind": 65.0,
      "pressure": 982.0,
      "source": "manual"
    }
  },
  {
    "method": "post",
    "path": "/api/reading",
    "json": {
      "station": "ArcticBase",
      "temp": -12.0,
      "feels": -18.0,
      "humidity": 92,
      "wind": 95.0,
      "pressure": 968.0,
      "source": "manual"
    }
  },
  {
    "method": "get",
    "path": "/api/alert-queue"
  }
]


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_smoke_endpoints():
    for case in SMOKE:
        fn = getattr(client, case["method"])
        kwargs = {"json": case["json"]} if "json" in case else {}
        r = fn(case["path"], **kwargs)
        assert r.status_code < 500, f"{case['path']} -> {r.status_code}: {r.text}"
