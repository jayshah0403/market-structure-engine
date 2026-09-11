"""HTTP layer: the cache-then-compute API (V2_SPEC PR 4).

This is the **only** module that imports FastAPI. The engine raises its own
exceptions and returns plain dicts keyed on `daily_levels` columns; translating
those into status codes and Pydantic models happens here and nowhere else
(V2_SPEC section 4, "Errors").

Shape of every session request:

    instrument lookup  ->  404 if unknown or inactive
    date check         ->  404 if the session is not yet published
    cache read         ->  hit only when engine_version == ENGINE_VERSION
    cold compute       ->  under a concurrency cap, then read back what persisted

The cold path is synchronous (V2_SPEC D3): a miss costs roughly 20 s while the
day's archive is downloaded and aggregated. What bounds it is the engine's own
`ARCHIVE_TIMEOUT_SECONDS` (60 s) on the archive request, so a download that
stalls fails instead of holding a compute slot open. There is no separate
request timeout at this layer.
"""

import datetime
import threading

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

import db
import ingest

# V2_SPEC D4. slowapi is the spec's named choice and the standard Starlette port
# of flask-limiter: it keys on the client address, understands FastAPI's
# dependency system, and needs no external store. Its counters live in this
# process, so they reset on redeploy and are per-replica — acceptable while
# Railway runs a single container, and the thing to revisit before scaling out.
RATE_LIMIT_PER_MINUTE = 30
RATE_LIMIT = f"{RATE_LIMIT_PER_MINUTE}/minute"

# V2_SPEC D4: at most this many archive downloads run at once. The cap protects
# memory (each compute peaks ~54 MB, CURRENT_STATE §3) rather than the database.
MAX_CONCURRENT_COLD_COMPUTES = 3
# How long a request waits for a slot before giving up. The client gets a 503 it
# can retry rather than a connection held open indefinitely.
COLD_COMPUTE_WAIT_SECONDS = 30

# Widest window the range endpoint will serve, in days inclusive. It never
# computes, so this bounds response size and the `missing` list rather than
# work: without it a decade-wide query builds a decade-long list of dates.
MAX_RANGE_DAYS = 365

cold_compute_slots = threading.BoundedSemaphore(MAX_CONCURRENT_COLD_COMPUTES)

limiter = Limiter(key_func=get_remote_address, default_limits=[RATE_LIMIT])

app = FastAPI(
    title="Market Structure Engine",
    version="1.0.0",
    description=(
        "Computed market-structure facts for completed sessions: daily levels, "
        "value area, initial balance, single prints and a day-type verdict.\n\n"
        "The archive publishes T+1, so the most recent available session is "
        "yesterday (UTC) and only after roughly 02:00 UTC."
    ),
)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)


def utc_today():
    """Today's UTC calendar date. A function so tests can freeze it."""
    return datetime.datetime.now(datetime.timezone.utc).date()


# --- response models ----------------------------------------------------------
# Pydantic lives here only. The engine keeps returning plain dicts, so no web
# dependency sits below the API layer (V2_SPEC PR 3, "As built").

class Health(BaseModel):
    status: str = Field(description="'ok', or 'degraded' if a dependency is down")
    db: str = Field(description="'ok' or 'fail'")
    engine_version: int


class Instrument(BaseModel):
    symbol: str
    exchange: str
    bucket_size: float
    period_seconds: int
    ib_periods: int
    archive_url_template: str
    active: bool


class SinglePrintRange(BaseModel):
    from_: float = Field(alias="from", description="Lowest bucket of the range")
    to: float = Field(description="Highest bucket of the range; equal to `from` "
                                  "for an isolated single print")

    model_config = {"populate_by_name": True}


class DailyLevels(BaseModel):
    """One computed session — the `daily_levels` row of V2_SPEC 2.2."""

    symbol: str
    session_date: datetime.date
    engine_version: int
    computed_at: datetime.datetime | None = None
    day_type: str
    reason: str
    poc: float
    vah: float
    val: float
    ib_high: float
    ib_low: float
    day_high: float
    day_low: float
    poor_high: float | None = None
    poor_low: float | None = None
    buying_tail: float | None = None
    selling_tail: float | None = None
    extension_above: float
    extension_below: float
    up_conf: float
    down_conf: float
    single_prints: list[SinglePrintRange]
    profile: dict[str, int] = Field(
        description="Bucket price -> TPO count. The primitive composites merge.")
    composite_id: int | None = Field(
        default=None, description="Set by PR 6; null means no composite yet.")


class SessionList(BaseModel):
    """Cached sessions in a window, plus the days that are not cached."""

    symbol: str
    from_: datetime.date = Field(alias="from")
    to: datetime.date
    sessions: list[DailyLevels]
    missing: list[datetime.date] = Field(
        description="Dates in the window with no fresh cached row. This endpoint "
                    "never computes, so these stay missing until requested "
                    "individually.")

    model_config = {"populate_by_name": True}


class SessionReport(BaseModel):
    symbol: str
    session_date: datetime.date
    report: str = Field(description="The rendered plain-text session report.")


# --- shared lookups -----------------------------------------------------------

def active_instrument(symbol: str):
    """The instruments row, or 404 when unknown or inactive.

    Owner decision at PR 4: `active` is an HTTP concern. The engine stays
    neutral on it — `db.get_instrument` returns inactive rows too.
    """
    instrument = db.get_instrument(symbol)
    if instrument is None or not instrument["active"]:
        raise HTTPException(status_code=404, detail=f"unknown symbol: {symbol}")
    return instrument


def _fresh(row):
    """A cached row counts as a hit only at the current engine version."""
    return row is not None and row["engine_version"] == ingest.ENGINE_VERSION


def _compute(symbol, session_date):
    """Run the cold path under the concurrency cap, translating engine errors."""
    if not cold_compute_slots.acquire(timeout=COLD_COMPUTE_WAIT_SECONDS):
        raise HTTPException(
            status_code=503,
            detail="too many sessions computing; retry shortly",
            headers={"Retry-After": "30"},
        )
    try:
        ingest.compute_day(symbol, session_date)
    except ingest.SessionNotPublished:
        # Owner decision at PR 4: the archive is not up yet, which is the same
        # condition as asking for today — absent, not broken.
        raise HTTPException(status_code=404, detail="session not yet published")
    except ingest.NoTradeData:
        raise HTTPException(
            status_code=404, detail="no trades in the archive for that session")
    except ingest.UnknownInstrument:
        raise HTTPException(status_code=404, detail=f"unknown symbol: {symbol}")
    except ingest.ArchiveUnavailable as exc:
        raise HTTPException(
            status_code=503, detail=f"archive unavailable: {exc}",
            headers={"Retry-After": "60"})
    finally:
        cold_compute_slots.release()

    # Read back rather than returning what compute_day handed us: the row that
    # went in picks up `computed_at` from the schema default, so only the stored
    # copy is the whole row.
    stored = db.get_daily_levels(symbol, session_date)
    if stored is None:
        raise HTTPException(
            status_code=503, detail="session computed but could not be read back")
    return stored


def session_row(symbol: str, session_date: datetime.date):
    """Cache-then-compute for one session. The path both session routes share."""
    active_instrument(symbol)

    if session_date >= utc_today():
        raise HTTPException(status_code=404, detail="session not yet published")

    cached = db.get_daily_levels(symbol, session_date)
    if _fresh(cached):
        return cached
    return _compute(symbol, session_date)


# --- routes -------------------------------------------------------------------

@app.get("/v1/health", response_model=Health, tags=["meta"],
         summary="Liveness plus a storage round-trip")
@limiter.exempt
def health(request: Request):
    """Returns 503 when storage is unreachable, so a platform health check sees
    the failure without parsing the body."""
    try:
        db.ping()
    except Exception:
        return JSONResponse(
            status_code=503,
            content=Health(status="degraded", db="fail",
                           engine_version=ingest.ENGINE_VERSION).model_dump(),
        )
    return Health(status="ok", db="ok", engine_version=ingest.ENGINE_VERSION)


@app.get("/v1/instruments", response_model=list[Instrument], tags=["meta"],
         summary="Instruments the engine is configured for")
def instruments(request: Request):
    """Every seeded instrument, including inactive ones, with the parameters the
    engine reads: bucket size, period width, IB length and archive template."""
    return db.list_instruments()


@app.get("/v1/sessions/{symbol}", response_model=SessionList, tags=["sessions"],
         summary="Cached sessions in a date window")
def list_sessions(
    request: Request,
    symbol: str,
    from_date: datetime.date = Query(alias="from",
                                     description="First session date, inclusive"),
    to_date: datetime.date = Query(alias="to",
                                   description="Last session date, inclusive"),
):
    """Reads the cache only — **never** triggers a compute (V2_SPEC PR 4).

    Days with no cached row, and days cached at a stale `engine_version`, are
    both reported in `missing`. Request one of those individually to compute it.

    The window is inclusive on both ends and may span at most `MAX_RANGE_DAYS`
    days; wider is a 422.
    """
    active_instrument(symbol)
    if from_date > to_date:
        raise HTTPException(
            status_code=422, detail="`from` must not be after `to`")

    span = (to_date - from_date).days + 1
    if span > MAX_RANGE_DAYS:
        raise HTTPException(
            status_code=422,
            detail=f"window spans {span} days; the maximum is {MAX_RANGE_DAYS}",
        )

    fresh = {row["session_date"]: row
             for row in db.list_daily_levels(symbol, from_date, to_date)
             if _fresh(row)}

    wanted = [from_date + datetime.timedelta(days=offset) for offset in range(span)]

    return SessionList(
        symbol=symbol,
        from_=from_date,
        to=to_date,
        sessions=[fresh[day] for day in wanted if day in fresh],
        missing=[day for day in wanted if day not in fresh],
    )


@app.get("/v1/sessions/{symbol}/{session_date}", response_model=DailyLevels,
         tags=["sessions"], summary="One computed session")
def get_session(request: Request, symbol: str, session_date: datetime.date):
    """Cached rows return in milliseconds. A miss computes the session inline,
    which takes roughly **20 seconds** while the day's archive is downloaded and
    aggregated (V2_SPEC D3). A download that stalls fails after 60 s rather than
    holding the request open.

    A row cached at an older `engine_version` is recomputed and overwritten.
    """
    return session_row(symbol, session_date)


@app.get("/v1/sessions/{symbol}/{session_date}/report",
         response_model=SessionReport, tags=["sessions"],
         summary="One session as a plain-text report")
def get_session_report(request: Request, symbol: str,
                       session_date: datetime.date):
    """The same cache-then-compute path as the session route, rendered as the
    written report instead of JSON — so a miss costs the same ~20 seconds."""
    row = session_row(symbol, session_date)
    return SessionReport(
        symbol=row["symbol"],
        session_date=row["session_date"],
        report=ingest.generate_report(row),
    )
