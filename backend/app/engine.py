"""VITALS domain engine — real environmental early-warning over live weather stations.

Architecture:

  Data source  -> REAL current weather via sources.open_meteo(STATIONS).
                  Cached 30 s; returns [] when offline.  Buffer holds the most
                  recent readings; generate_event() pops ONE station reading
                  (tagged source="live") or emits a synthetic fallback
                  (source="fallback") so the stream never stalls.

  generate_event()  Polls open_meteo when the buffer runs low (rate-limited by
                    REFILL_INTERVAL so the 200-cycle pytest loop never blocks
                    on network I/O).  Returns one enriched station reading.

  process()         NEWS2-style banded early-warning score adapted to
                    environmental signals:
                      temp, feels_like, wind_speed, humidity, pressure
                      -> 0-3 risk points each via threshold bands
                      -> aggregate score -> green/amber/red triage
                      + trend detection over a per-station rolling window.
                    When aillm.available() and severity != "ok" a one-line LLM
                    risk advisory is generated; otherwise a deterministic
                    summary is returned.

  kpis()            stations_monitored, critical_alerts, avg_score,
                    deteriorating, mean_time_to_alert_ms.

  snapshot()        {"recent": [...], "series": {"alert_rate": [...],
                                                  "avg_score": [...]}}
"""
import math
import random
import time
from collections import deque

try:
    from .sources import open_meteo
except Exception:  # noqa: BLE001 — offline / missing dep
    def open_meteo(stations): return []  # type: ignore[misc]

try:
    from . import aillm
except Exception:  # noqa: BLE001
    class aillm:  # type: ignore[no-redef]
        @staticmethod
        def available(): return False
        @staticmethod
        def complete(prompt, system="", max_tokens=120): return None

# ---------------------------------------------------------------------------
# Station roster — 10 major-city weather stations (real coordinates)
# ---------------------------------------------------------------------------
STATIONS = [
    ("Chennai",    13.08,   80.27),
    ("London",     51.51,   -0.12),
    ("New York",   40.71,  -74.00),
    ("Tokyo",      35.68,  139.69),
    ("Sydney",    -33.87,  151.21),
    ("Dubai",      25.20,   55.27),
    ("Singapore",   1.35,  103.80),
    ("Berlin",     52.52,   13.40),
    ("Sao Paulo", -23.55,  -46.63),
    ("Nairobi",    -1.29,   36.82),
]

# ---------------------------------------------------------------------------
# Environmental banded thresholds  (0-3 risk points per signal)
#
# Temperature °C
#   extreme cold ≤-10 or extreme heat ≥45  -> 3
#   cold -10..0  or  hot 38..44            -> 2
#   cool 0..10   or  warm 32..37           -> 1
#   comfortable 10..32                     -> 0
#
# Feels-like °C  (same bands as temp — captures wind-chill / heat index)
#   ≤-15 or ≥50  -> 3
#   -15..-5 or 43..49 -> 2
#   -5..5 or 38..42   -> 1
#   5..38              -> 0
#
# Wind speed km/h
#   ≥90 (violent storm)  -> 3
#   62..89 (storm)       -> 2
#   38..61 (strong gale) -> 1
#   0..37 (normal)       -> 0
#
# Relative humidity %
#   ≥95 (saturation) or <5 (extreme dry)       -> 3
#   90..94 or 5..9                             -> 2
#   80..89 or 10..19                           -> 1
#   20..79 (comfortable)                        -> 0
#
# Pressure hPa
#   ≤970 (deep low) or ≥1040 (very high)       -> 3
#   970..985 or 1030..1039                      -> 2
#   985..995 or 1020..1029                      -> 1
#   995..1020 (normal)                          -> 0
# ---------------------------------------------------------------------------

def _score_temp(t: float) -> int:
    if t <= -10 or t >= 45:
        return 3
    if t <= 0 or t >= 38:
        return 2
    if t <= 10 or t >= 32:
        return 1
    return 0


def _score_feels(f: float) -> int:
    if f <= -15 or f >= 50:
        return 3
    if f <= -5 or f >= 43:
        return 2
    if f <= 5 or f >= 38:
        return 1
    return 0


def _score_wind(w: float) -> int:
    if w >= 90:
        return 3
    if w >= 62:
        return 2
    if w >= 38:
        return 1
    return 0


def _score_humidity(h: float) -> int:
    if h >= 95 or h < 5:
        return 3
    if h >= 90 or h < 10:
        return 2
    if h >= 80 or h < 20:
        return 1
    return 0


def _score_pressure(p: float) -> int:
    if p <= 970 or p >= 1040:
        return 3
    if p <= 985 or p >= 1030:
        return 2
    if p <= 995 or p >= 1020:
        return 1
    return 0


ENV_SCORERS = [
    ("temp",     _score_temp),
    ("feels",    _score_feels),
    ("wind",     _score_wind),
    ("humidity", _score_humidity),
    ("pressure", _score_pressure),
]

# ---------------------------------------------------------------------------
# Fallback synthetic readings — used ONLY when open_meteo returns []
# ---------------------------------------------------------------------------
# Typical-range centres and small jitter per station (no physiological meaning)
_STATION_NORMAL = {
    "Chennai":   {"temp": 32, "humidity": 75, "wind": 12, "pressure": 1008, "feels": 38},
    "London":    {"temp": 12, "humidity": 78, "wind": 18, "pressure": 1013, "feels": 9},
    "New York":  {"temp": 15, "humidity": 60, "wind": 20, "pressure": 1015, "feels": 13},
    "Tokyo":     {"temp": 18, "humidity": 65, "wind": 14, "pressure": 1012, "feels": 17},
    "Sydney":    {"temp": 21, "humidity": 60, "wind": 16, "pressure": 1014, "feels": 20},
    "Dubai":     {"temp": 38, "humidity": 55, "wind": 20, "pressure": 1000, "feels": 42},
    "Singapore": {"temp": 30, "humidity": 82, "wind": 10, "pressure": 1009, "feels": 35},
    "Berlin":    {"temp": 10, "humidity": 72, "wind": 15, "pressure": 1015, "feels": 8},
    "Sao Paulo": {"temp": 24, "humidity": 70, "wind": 12, "pressure": 1012, "feels": 26},
    "Nairobi":   {"temp": 20, "humidity": 65, "wind": 10, "pressure": 1018, "feels": 19},
}
_DEFAULT_NORMAL = {"temp": 20, "humidity": 65, "wind": 10, "pressure": 1013, "feels": 18}


def _make_fallback(name: str, lat: float, lon: float, rng: random.Random) -> dict:
    base = _STATION_NORMAL.get(name, _DEFAULT_NORMAL)
    jitter = lambda mu, s: round(rng.gauss(mu, s), 1)
    return {
        "station":  name,
        "lat":      lat,
        "lon":      lon,
        "temp":     jitter(base["temp"], 3),
        "feels":    jitter(base["feels"], 3),
        "humidity": min(100, max(0, round(jitter(base["humidity"], 5)))),
        "wind":     max(0, jitter(base["wind"], 4)),
        "pressure": jitter(base["pressure"], 5),
        "ts":       time.time(),
        "source":   "fallback",
    }


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------
TREND_WINDOW   = 6     # rolling window depth for per-station trend detection
SERIES_CAP     = 90    # max series length
REFILL_INTERVAL = 30.0 # minimum seconds between open_meteo fetches
BUFFER_LOW     = 2     # refill when fewer station readings remain in buffer


class Engine:
    """Stream-processing facade implementing the shared runtime contract.

    Live data flow:
      - _station_buffer holds un-processed station readings from the last
        open_meteo() call; generate_event() pops ONE reading per call.
      - When buffer is low, _refill_buffer() attempts a fresh fetch but only
        if REFILL_INTERVAL seconds have elapsed (keeps the 200-cycle pytest
        test fast and network-free).
      - If the buffer is empty (offline / rate-limited) a synthetic fallback
        reading is emitted instead (source="fallback").
    """

    def __init__(self) -> None:
        self.rng = random.Random(time.time_ns() & 0xFFFFFFFF)
        # Station buffer: list of live readings waiting to be processed
        self._station_buffer: list[dict] = []
        self._last_refill: float = 0.0
        # Per-station rolling score history for trend detection
        self._score_history: dict[str, deque] = {}
        # Per-station latest processed result
        self._latest: dict[str, dict] = {}
        # Alert queue (all stations ranked by aggregate score)
        self._alert_queue: list[dict] = []
        # KPI accumulators
        self._critical_alerts: int = 0
        self._alert_timestamps: deque = deque(maxlen=500)
        self._score_series: deque = deque(maxlen=500)
        self._alert_rate_series: deque = deque(maxlen=SERIES_CAP)
        self._avg_score_series: deque = deque(maxlen=SERIES_CAP)
        self.recent: deque = deque(maxlen=120)
        # Track which stations are "deteriorating" (score rising trend)
        self._deteriorating: set[str] = set()

    # ------------------------------------------------------------------
    # Buffer management
    # ------------------------------------------------------------------
    def _refill_buffer(self) -> None:
        """Fetch fresh station readings from open_meteo — rate-limited."""
        now = time.time()
        if now - self._last_refill < REFILL_INTERVAL:
            return
        self._last_refill = now
        try:
            readings = open_meteo(STATIONS)
        except Exception:  # noqa: BLE001
            readings = []
        if readings:
            for r in readings:
                r["source"] = "live"
            self._station_buffer = readings  # replace buffer with fresh batch

    def _pop_station(self) -> dict | None:
        if len(self._station_buffer) <= BUFFER_LOW:
            self._refill_buffer()
        if self._station_buffer:
            return self._station_buffer.pop(0)
        return None

    # ------------------------------------------------------------------
    # generate_event
    # ------------------------------------------------------------------
    def generate_event(self) -> dict:
        """Emit one station reading — live (source='live') or synthetic fallback."""
        reading = self._pop_station()
        if reading:
            return reading
        # Offline / buffer drained: emit a synthetic fallback for a random station
        name, lat, lon = self.rng.choice(STATIONS)
        return _make_fallback(name, lat, lon, self.rng)

    # ------------------------------------------------------------------
    # Scoring (pure computation — no state mutation)
    # ------------------------------------------------------------------
    def _compute_score(self, event: dict) -> dict:
        """Map environmental signals to 0-3 risk points; sum to aggregate score."""
        contributions: dict[str, int] = {}
        max_single = 0
        for key, scorer in ENV_SCORERS:
            val = event.get(key)
            if val is None:
                pts = 0
            else:
                try:
                    pts = scorer(float(val))
                except Exception:  # noqa: BLE001
                    pts = 0
            contributions[key] = pts
            max_single = max(max_single, pts)

        score = sum(contributions.values())

        # Triage mirrors NEWS2 logic (same thresholds)
        if score >= 7:
            triage = "red"
        elif score >= 3 or max_single == 3:
            triage = "amber"
        else:
            triage = "green"

        severity = {"green": "ok", "amber": "warn", "red": "critical"}[triage]
        return {
            "score": score,
            "triage": triage,
            "severity": severity,
            "contributions": contributions,
        }

    # ------------------------------------------------------------------
    # process
    # ------------------------------------------------------------------
    def process(self, event: dict) -> dict:
        """Score a station reading, detect trends, return enriched result.

        Returned dict includes at minimum:
          severity: "ok" | "warn" | "critical"
          summary:  short human-readable string for the feed
        Plus: score, triage, trend, contributions, station.
        """
        station = event.get("station", "UNKNOWN")
        t0 = time.perf_counter()

        scored = self._compute_score(event)
        score = scored["score"]

        # Trend detection over per-station rolling window
        if station not in self._score_history:
            self._score_history[station] = deque(maxlen=TREND_WINDOW)
        history = self._score_history[station]

        trend = "stable"
        if len(history) >= 3:
            recent_avg = sum(list(history)[-3:]) / 3
            older_avg = (sum(list(history)[:-3]) / max(len(history) - 3, 1)
                         if len(history) > 3 else recent_avg)
            diff = recent_avg - older_avg
            if diff >= 1.5:
                trend = "rising"
            elif diff <= -1.5:
                trend = "falling"

        history.append(score)

        # Track deteriorating stations (rising trend)
        if trend == "rising":
            self._deteriorating.add(station)
        elif trend == "falling" or scored["severity"] == "ok":
            self._deteriorating.discard(station)

        # Build summary — LLM advisory when available and non-green, else deterministic
        base_summary = (
            "%s score=%d [%s] trend=%s "
            "T=%.1f°C feels=%.1f wind=%.1f hum=%d%% P=%.1f hPa"
        ) % (
            station, score, scored["triage"].upper(), trend,
            event.get("temp", 0) or 0,
            event.get("feels", 0) or 0,
            event.get("wind", 0) or 0,
            int(event.get("humidity", 0) or 0),
            event.get("pressure", 0) or 0,
        )

        if aillm.available() and scored["severity"] != "ok":
            llm_system = (
                "You are a concise site-health analyst. "
                "Give a ONE-LINE risk advisory (max 20 words) for the alert."
            )
            llm_prompt = (
                "Station %s: aggregate risk score %d (%s), trend %s. "
                "Temp %.1f°C, feels %.1f°C, wind %.1f km/h, humidity %d%%, pressure %.1f hPa. "
                "One-line advisory:"
            ) % (
                station, score, scored["triage"],
                trend,
                event.get("temp", 0) or 0,
                event.get("feels", 0) or 0,
                event.get("wind", 0) or 0,
                int(event.get("humidity", 0) or 0),
                event.get("pressure", 0) or 0,
            )
            advisory = aillm.complete(llm_prompt, system=llm_system, max_tokens=120)
            summary = advisory if advisory else base_summary
        else:
            summary = base_summary

        out = {
            **event,
            "score": score,
            "triage": scored["triage"],
            "severity": scored["severity"],
            "trend": trend,
            "contributions": scored["contributions"],
            "latency_ms": round((time.perf_counter() - t0) * 1000, 3),
            "summary": summary,
        }

        # Update KPIs / series
        self.recent.append(out)
        self._score_series.append(score)
        if scored["severity"] == "critical":
            self._critical_alerts += 1
            self._alert_timestamps.append(time.time())

        self._latest[station] = out
        self._rebuild_alert_queue()

        k = self.kpis()
        self._alert_rate_series.append(
            k["critical_alerts"] / max(k["stations_monitored"], 1) * 100
        )
        self._avg_score_series.append(k["avg_score"])

        return out

    def _rebuild_alert_queue(self) -> None:
        """Rebuild alert queue sorted by descending risk score."""
        rows = []
        for st, latest in self._latest.items():
            rows.append({
                "station":       st,
                "score":         latest.get("score", 0),
                "triage":        latest.get("triage", "green"),
                "severity":      latest.get("severity", "ok"),
                "trend":         latest.get("trend", "stable"),
                "temp":          latest.get("temp"),
                "feels":         latest.get("feels"),
                "wind":          latest.get("wind"),
                "humidity":      latest.get("humidity"),
                "pressure":      latest.get("pressure"),
                "source":        latest.get("source", "live"),
                "summary":       latest.get("summary", ""),
            })
        rows.sort(key=lambda r: (-r["score"], r["station"]))
        self._alert_queue = rows

    # ------------------------------------------------------------------
    # kpis
    # ------------------------------------------------------------------
    def kpis(self) -> dict:
        stations_monitored = len(self._latest)
        critical_alerts    = self._critical_alerts
        avg_score = (
            round(sum(self._score_series) / len(self._score_series), 2)
            if self._score_series else 0.0
        )
        deteriorating = len(self._deteriorating)
        # Mean time-to-alert: average inter-alert gap in ms over recent critical alerts
        ts_list = list(self._alert_timestamps)
        if len(ts_list) >= 2:
            gaps = [(ts_list[i] - ts_list[i - 1]) * 1000
                    for i in range(1, len(ts_list))]
            mtta = round(sum(gaps) / len(gaps), 1)
        else:
            mtta = 0.0
        return {
            "stations_monitored":    stations_monitored,
            "critical_alerts":       critical_alerts,
            "avg_score":             avg_score,
            "deteriorating":         deteriorating,
            "mean_time_to_alert_ms": mtta,
        }

    # ------------------------------------------------------------------
    # snapshot
    # ------------------------------------------------------------------
    def snapshot(self) -> dict:
        return {
            "recent": list(self.recent)[-30:],
            "series": {
                "alert_rate": list(self._alert_rate_series),
                "avg_score":  list(self._avg_score_series),
            },
        }

    # ------------------------------------------------------------------
    # Domain API helpers (called from core.py)
    # ------------------------------------------------------------------
    def submit_reading(self, reading: dict) -> dict:
        """Accept an externally supplied station reading and score it."""
        if "ts" not in reading:
            reading = {**reading, "ts": time.time()}
        return self.process(reading)

    def station_state(self, name: str) -> dict | None:
        """Return the latest scored state for a station, or None."""
        return self._latest.get(name)

    def alert_queue(self, limit: int = 50) -> list:
        """Return stations ranked by risk score (highest first)."""
        return self._alert_queue[:limit]


engine = Engine()
