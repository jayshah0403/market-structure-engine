"""Acceptance tests for V2_SPEC PR 4 (cache-then-compute API).

Every test here is pure. `db`'s four read functions and `ingest.compute_day` are
replaced by `FakeStore`, an in-memory stand-in, so nothing reaches Postgres or
data.binance.vision and the whole file runs in CI with no `CONNECTION_STRING`.

`api` is the only module under test that imports FastAPI (V2_SPEC section 4);
the engine keeps returning plain dicts and the Pydantic models live here-side of
that line only.
"""

import datetime
import json
import os
import threading

import pytest
from fastapi.testclient import TestClient

import api
import ingest

FIXTURE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
OPENAPI_SNAPSHOT = os.path.join(FIXTURE_DIR, "openapi_v1.json")

# Frozen "today" so the future/today rules are not a function of the wall clock.
TODAY = datetime.date(2026, 9, 11)
YESTERDAY = datetime.date(2026, 9, 10)
CACHED_DAY = datetime.date(2026, 7, 7)
UNCACHED_DAY = datetime.date(2026, 7, 8)

# Shaped exactly like the seeded instruments row (CURRENT_STATE §2).
BTCUSDT = {
    "symbol": "BTCUSDT",
    "exchange": "binance-spot",
    "bucket_size": 25.0,
    "period_seconds": 1800,
    "ib_periods": 2,
    "archive_url_template": "https://example.invalid/{symbol}/{date}.zip",
    "active": True,
}
# An inactive instrument: reachable in the table, 404 through the API.
RETIRED = dict(BTCUSDT, symbol="OLDCOIN", active=False)


def _row(session_date, engine_version=None, poc=63250.0):
    """A `daily_levels` row shaped exactly as `db.get_daily_levels` returns one.

    Every column of V2_SPEC 2.2, with NUMERIC already floats and JSONB already
    parsed, which is what db.py's read conventions promise.
    """
    return {
        "symbol": "BTCUSDT",
        "session_date": session_date,
        "engine_version": (
            ingest.ENGINE_VERSION if engine_version is None else engine_version),
        "computed_at": datetime.datetime(
            2026, 9, 11, 1, 34, tzinfo=datetime.timezone.utc),
        "day_type": "Neutral",
        "reason": "Extended both sides (0.9x up, 4.1x down) — two-sided, responsive",
        "poc": poc,
        "vah": 63825.0,
        "val": 63050.0,
        "ib_high": 64300.0,
        "ib_low": 63975.0,
        "day_high": 64300.0,
        "day_low": 62650.0,
        "poor_high": None,
        "poor_low": 62650.0,
        "buying_tail": None,
        "selling_tail": 64300.0,
        "extension_above": 0.0,
        "extension_below": 4.08,
        "up_conf": 0.6,
        "down_conf": 0.55,
        "single_prints": [{"from": 62650, "to": 62675}],
        "profile": {"63250": 19, "63225": 18},
        "composite_id": None,
    }


class FakeStore:
    """In-memory stand-in for db.py's reads and for `ingest.compute_day`.

    `compute_day` writes into the same dict the reads see, so the miss path
    behaves like the real one: compute persists, and the route reads back what
    was persisted.
    """

    def __init__(self):
        self.instruments = {"BTCUSDT": dict(BTCUSDT), "OLDCOIN": dict(RETIRED)}
        self.rows = {}
        self.compute_calls = []
        self.compute_error = None
        self.db_up = True

    def _check(self):
        if not self.db_up:
            raise RuntimeError("connection refused")

    # --- the db.py surface the API layer uses -------------------------------

    def ping(self):
        self._check()
        return True

    def get_instrument(self, symbol):
        self._check()
        return self.instruments.get(symbol)

    def list_instruments(self):
        self._check()
        return [dict(row) for _symbol, row in sorted(self.instruments.items())]

    def get_daily_levels(self, symbol, session_date):
        self._check()
        return self.rows.get((symbol, session_date))

    def list_daily_levels(self, symbol, from_date, to_date):
        self._check()
        return [row for (sym, day), row in sorted(self.rows.items())
                if sym == symbol and from_date <= day <= to_date]

    # --- the engine entry point ---------------------------------------------

    def compute_day(self, symbol, session_date):
        self.compute_calls.append((symbol, session_date))
        if self.compute_error is not None:
            raise self.compute_error
        row = _row(session_date)
        self.rows[(symbol, session_date)] = row
        return row


@pytest.fixture
def store(monkeypatch):
    fake = FakeStore()
    for name in ("ping", "get_instrument", "list_instruments",
                 "get_daily_levels", "list_daily_levels"):
        monkeypatch.setattr(api.db, name, getattr(fake, name))
    monkeypatch.setattr(api.ingest, "compute_day", fake.compute_day)
    monkeypatch.setattr(api, "utc_today", lambda: TODAY)
    # One test exercises the limiter deliberately; leaving it on everywhere else
    # would make the file order-dependent and burn the per-minute budget.
    monkeypatch.setattr(api.limiter, "enabled", False)
    return fake


@pytest.fixture
def client(store):
    with TestClient(api.app) as test_client:
        yield test_client


# --- cache hit vs cache miss --------------------------------------------------

def test_cache_hit_returns_the_stored_row_without_computing(client, store):
    """V2_SPEC PR 4 acceptance: hit path returns the cached row, no compute."""
    store.rows[("BTCUSDT", CACHED_DAY)] = _row(CACHED_DAY)

    response = client.get("/v1/sessions/BTCUSDT/%s" % CACHED_DAY)

    assert response.status_code == 200
    assert store.compute_calls == []
    body = response.json()
    assert body["symbol"] == "BTCUSDT"
    assert body["session_date"] == CACHED_DAY.isoformat()
    assert body["poc"] == 63250.0
    assert body["engine_version"] == ingest.ENGINE_VERSION


def test_cache_miss_computes_exactly_once_and_returns_the_persisted_row(client, store):
    """V2_SPEC PR 4 acceptance: miss path calls compute once, returns the row."""
    response = client.get("/v1/sessions/BTCUSDT/%s" % UNCACHED_DAY)

    assert response.status_code == 200
    assert store.compute_calls == [("BTCUSDT", UNCACHED_DAY)]
    assert response.json()["session_date"] == UNCACHED_DAY.isoformat()
    # The response is what landed in storage, not a value computed and dropped.
    assert ("BTCUSDT", UNCACHED_DAY) in store.rows
    assert response.json()["poc"] == store.rows[("BTCUSDT", UNCACHED_DAY)]["poc"]


def test_a_stale_engine_version_recomputes_and_overwrites(client, store):
    """V2_SPEC PR 4: a hit requires engine_version == ENGINE_VERSION."""
    store.rows[("BTCUSDT", CACHED_DAY)] = _row(
        CACHED_DAY, engine_version=ingest.ENGINE_VERSION - 1, poc=1.0)

    response = client.get("/v1/sessions/BTCUSDT/%s" % CACHED_DAY)

    assert response.status_code == 200
    assert store.compute_calls == [("BTCUSDT", CACHED_DAY)]
    assert response.json()["engine_version"] == ingest.ENGINE_VERSION
    assert response.json()["poc"] == 63250.0
    # Overwritten, not left beside the stale row.
    assert (store.rows[("BTCUSDT", CACHED_DAY)]["engine_version"]
            == ingest.ENGINE_VERSION)


# --- validation ---------------------------------------------------------------

def test_a_malformed_date_is_rejected_as_422(client, store):
    response = client.get("/v1/sessions/BTCUSDT/not-a-date")

    assert response.status_code == 422
    assert store.compute_calls == []


def test_an_impossible_date_is_rejected_as_422(client, store):
    response = client.get("/v1/sessions/BTCUSDT/2026-13-45")

    assert response.status_code == 422
    assert store.compute_calls == []


def test_today_is_not_yet_published(client, store):
    """The archive publishes T+1, so the current session never exists."""
    response = client.get("/v1/sessions/BTCUSDT/%s" % TODAY)

    assert response.status_code == 404
    assert store.compute_calls == []


def test_a_future_date_is_not_yet_published(client, store):
    response = client.get(
        "/v1/sessions/BTCUSDT/%s" % (TODAY + datetime.timedelta(days=1)))

    assert response.status_code == 404
    assert store.compute_calls == []


def test_an_unknown_symbol_is_404(client, store):
    response = client.get("/v1/sessions/NOPE/%s" % CACHED_DAY)

    assert response.status_code == 404
    assert store.compute_calls == []


def test_an_inactive_symbol_is_404(client, store):
    """Owner decision at PR 4: `active` is an HTTP concern, not an engine one."""
    response = client.get("/v1/sessions/OLDCOIN/%s" % CACHED_DAY)

    assert response.status_code == 404
    assert store.compute_calls == []


# --- engine failures, translated ----------------------------------------------

def test_an_archive_failure_is_503(client, store):
    store.compute_error = ingest.ArchiveUnavailable("upstream 500")

    response = client.get("/v1/sessions/BTCUSDT/%s" % UNCACHED_DAY)

    assert response.status_code == 503
    assert store.compute_calls == [("BTCUSDT", UNCACHED_DAY)]


def test_an_unpublished_past_session_is_404(client, store):
    """Owner decision at PR 4: same condition as a future date, so same code."""
    store.compute_error = ingest.SessionNotPublished("not up yet")

    response = client.get("/v1/sessions/BTCUSDT/%s" % YESTERDAY)

    assert response.status_code == 404


def test_an_empty_archive_is_404(client, store):
    store.compute_error = ingest.NoTradeData("no trades")

    response = client.get("/v1/sessions/BTCUSDT/%s" % UNCACHED_DAY)

    assert response.status_code == 404


def test_a_failed_compute_persists_nothing(client, store):
    store.compute_error = ingest.ArchiveUnavailable("upstream 500")

    client.get("/v1/sessions/BTCUSDT/%s" % UNCACHED_DAY)

    assert store.rows == {}


# --- the range endpoint -------------------------------------------------------

def test_range_returns_cached_rows_and_names_the_missing_dates(client, store):
    """V2_SPEC PR 4: cached rows in range + missing: [dates]."""
    store.rows[("BTCUSDT", datetime.date(2026, 7, 7))] = _row(
        datetime.date(2026, 7, 7))
    store.rows[("BTCUSDT", datetime.date(2026, 7, 9))] = _row(
        datetime.date(2026, 7, 9))

    response = client.get("/v1/sessions/BTCUSDT?from=2026-07-07&to=2026-07-09")

    assert response.status_code == 200
    body = response.json()
    assert body["symbol"] == "BTCUSDT"
    assert body["from"] == "2026-07-07"
    assert body["to"] == "2026-07-09"
    assert [s["session_date"] for s in body["sessions"]] == [
        "2026-07-07", "2026-07-09"]
    assert body["missing"] == ["2026-07-08"]


def test_range_never_triggers_a_compute(client, store):
    """V2_SPEC PR 4: the range endpoint does **not** trigger compute."""
    response = client.get("/v1/sessions/BTCUSDT?from=2026-07-07&to=2026-07-09")

    assert response.status_code == 200
    assert store.compute_calls == []
    assert response.json()["sessions"] == []
    assert response.json()["missing"] == [
        "2026-07-07", "2026-07-08", "2026-07-09"]


def test_range_treats_a_stale_row_as_missing(client, store):
    """A stale row is not a cache hit, and the range endpoint cannot refresh it.

    V2_SPEC PR 4 states the freshness rule for the cache generally, so a row at
    the wrong engine_version is reported as missing rather than served silently.
    """
    store.rows[("BTCUSDT", CACHED_DAY)] = _row(
        CACHED_DAY, engine_version=ingest.ENGINE_VERSION - 1)

    response = client.get(
        "/v1/sessions/BTCUSDT?from=%s&to=%s" % (CACHED_DAY, CACHED_DAY))

    assert response.json()["sessions"] == []
    assert response.json()["missing"] == [CACHED_DAY.isoformat()]
    assert store.compute_calls == []


def test_range_rejects_an_inverted_window(client, store):
    response = client.get("/v1/sessions/BTCUSDT?from=2026-07-09&to=2026-07-07")

    assert response.status_code == 422


def test_range_rejects_an_unknown_symbol(client, store):
    response = client.get("/v1/sessions/NOPE?from=2026-07-07&to=2026-07-09")

    assert response.status_code == 404


# --- report -------------------------------------------------------------------

def test_report_renders_the_cached_session(client, store):
    store.rows[("BTCUSDT", CACHED_DAY)] = _row(CACHED_DAY)

    response = client.get("/v1/sessions/BTCUSDT/%s/report" % CACHED_DAY)

    assert response.status_code == 200
    body = response.json()
    assert body["symbol"] == "BTCUSDT"
    assert body["session_date"] == CACHED_DAY.isoformat()
    assert "Session Report" in body["report"]
    assert "63250" in body["report"]
    assert store.compute_calls == []


def test_report_computes_on_a_miss(client, store):
    response = client.get("/v1/sessions/BTCUSDT/%s/report" % UNCACHED_DAY)

    assert response.status_code == 200
    assert store.compute_calls == [("BTCUSDT", UNCACHED_DAY)]


# --- health and instruments ---------------------------------------------------

def test_health_reports_ok_when_the_database_answers(client, store):
    response = client.get("/v1/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok", "db": "ok", "engine_version": ingest.ENGINE_VERSION}


def test_health_reports_a_failing_database_with_503(client, store):
    """Owner decision at PR 4: the status code fails too, so a platform health
    check catches it without parsing the body."""
    store.db_up = False

    response = client.get("/v1/health")

    assert response.status_code == 503
    body = response.json()
    assert body["db"] == "fail"
    assert body["status"] == "degraded"
    assert body["engine_version"] == ingest.ENGINE_VERSION


def test_instruments_lists_the_seeded_configuration(client, store):
    response = client.get("/v1/instruments")

    assert response.status_code == 200
    symbols = [row["symbol"] for row in response.json()]
    assert "BTCUSDT" in symbols
    btc = next(row for row in response.json() if row["symbol"] == "BTCUSDT")
    assert btc["bucket_size"] == 25.0
    assert btc["period_seconds"] == 1800
    assert btc["ib_periods"] == 2
    assert btc["active"] is True


# --- the v1 routes are gone ---------------------------------------------------

def test_the_millisecond_routes_no_longer_exist(client):
    """V2_SPEC PR 4: old /profile and /report routes removed."""
    assert client.get("/profile/1783382400000").status_code == 404
    assert client.get("/report/1783382400000").status_code == 404


# --- typed schemas ------------------------------------------------------------

def _openapi_projection(spec):
    """Route -> status -> response schema name.

    A projection rather than the whole document: the full OpenAPI JSON churns
    with every FastAPI release, while what the acceptance criterion actually
    asks is that every route answers with a *named* schema rather than a bare
    object.
    """
    projection = {}
    for path, methods in spec["paths"].items():
        for method, operation in methods.items():
            responses = {}
            for code, response in operation.get("responses", {}).items():
                schema = (response.get("content", {})
                          .get("application/json", {}).get("schema", {}))
                ref = schema.get("$ref") or schema.get("items", {}).get("$ref")
                responses[code] = ref
            projection["%s %s" % (method.upper(), path)] = responses
    return projection


def test_openapi_matches_the_committed_snapshot(client):
    """V2_SPEC PR 4 acceptance: /docs renders typed schemas."""
    with open(OPENAPI_SNAPSHOT, encoding="utf-8") as handle:
        expected = json.load(handle)

    assert _openapi_projection(client.get("/openapi.json").json()) == expected


def test_every_success_response_is_a_named_model(client):
    spec = client.get("/openapi.json").json()

    for route, responses in _openapi_projection(spec).items():
        assert responses.get("200"), "%s has an untyped 200" % route

    assert {"DailyLevels", "SessionList", "Health", "Instrument", "SessionReport"} <= \
        set(spec["components"]["schemas"])


# --- rate limiting and the cold-compute cap -----------------------------------

def test_the_rate_limit_returns_429_once_the_budget_is_spent(store, monkeypatch):
    """V2_SPEC D4: 30 requests per minute per IP."""
    monkeypatch.setattr(api.limiter, "enabled", True)
    api.limiter.reset()

    with TestClient(api.app) as test_client:
        codes = [test_client.get("/v1/instruments").status_code
                 for _ in range(api.RATE_LIMIT_PER_MINUTE + 1)]

    assert codes[:api.RATE_LIMIT_PER_MINUTE] == [200] * api.RATE_LIMIT_PER_MINUTE
    assert codes[-1] == 429


def test_health_is_exempt_from_the_rate_limit(store, monkeypatch):
    """A platform health check must not be able to lock out real traffic."""
    monkeypatch.setattr(api.limiter, "enabled", True)
    api.limiter.reset()

    with TestClient(api.app) as test_client:
        codes = [test_client.get("/v1/health").status_code
                 for _ in range(api.RATE_LIMIT_PER_MINUTE + 5)]

    assert set(codes) == {200}


def test_cold_computes_are_capped_and_the_overflow_is_503(store, monkeypatch):
    """V2_SPEC D4: at most 3 concurrent cold computes; here capped at 1.

    The first request is parked inside compute_day while the second arrives, so
    the second can only be answered by the cap.
    """
    running = threading.Event()
    release = threading.Event()

    def blocking_compute(symbol, session_date):
        store.compute_calls.append((symbol, session_date))
        running.set()
        release.wait(10)
        row = _row(session_date)
        store.rows[(symbol, session_date)] = row
        return row

    monkeypatch.setattr(api.ingest, "compute_day", blocking_compute)
    monkeypatch.setattr(api, "cold_compute_slots", threading.BoundedSemaphore(1))
    monkeypatch.setattr(api, "COLD_COMPUTE_WAIT_SECONDS", 0.1)

    first = {}

    def run_first():
        with TestClient(api.app) as test_client:
            first["code"] = test_client.get(
                "/v1/sessions/BTCUSDT/%s" % UNCACHED_DAY).status_code

    worker = threading.Thread(target=run_first)
    worker.start()
    try:
        assert running.wait(10), "the first compute never started"
        with TestClient(api.app) as test_client:
            second = test_client.get(
                "/v1/sessions/BTCUSDT/%s" % datetime.date(2026, 7, 20))
        assert second.status_code == 503
    finally:
        release.set()
        worker.join(10)

    assert first["code"] == 200
