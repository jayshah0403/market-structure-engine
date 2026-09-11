# Market Structure Engine

An API that builds the market profile of an instrument (currently BTC/USDT), identifies the key levels of that profile, classifies the day type, and returns it all as typed JSON.

## Sample Output

```
════════════════════════════════════════
BTCUSDT — Session Report, 2026-07-07
════════════════════════════════════════
Day Type   : Directional (unclassified)
Reason     : Extended 4.1x IB to the down; one-timeframing 0.60 — directional but not a clean trend
VALUE
POC         : 63250.0
Value Area  : 63050.0 - 63825.0
Initial Balance: 63975.0 - 64300.0
STRUCTURE
Extension: 0.00/4.08
Buying Tail: 62650
Selling Tail: 64300
Poor Low/High: None / None
════════════════════════════════════════
```

The same session as JSON, from `GET /v1/sessions/BTCUSDT/2026-07-07` (abridged — the full row also carries the TPO histogram):

```json
{
  "symbol": "BTCUSDT",
  "session_date": "2026-07-07",
  "engine_version": 1,
  "computed_at": "2026-09-11T01:34:00Z",
  "day_type": "Directional (unclassified)",
  "reason": "Extended 4.1x IB to the down; one-timeframing 0.60 — directional but not a clean trend",
  "poc": 63250.0,
  "vah": 63825.0,
  "val": 63050.0,
  "ib_high": 64300.0,
  "ib_low": 63975.0,
  "day_high": 64300.0,
  "day_low": 62650.0,
  "poor_high": null,
  "poor_low": null,
  "buying_tail": 62650.0,
  "selling_tail": 64300.0,
  "extension_above": 0.0,
  "extension_below": 4.08,
  "single_prints": [{ "from": 62650.0, "to": 62675.0 }]
}
```

## Why

I built this because there are currently no market profile APIs offering these services. If someone is building a trading bot and wants to trade around the key levels a market profile marks out, they can identify those levels with a single API call. And since Exocharts — the actual software — is expensive, this gives an alternative way to access the same data.

## How It Works

The pipeline, end to end:

- **Ingestion** — one streaming pass over the day's `data.binance.vision` aggTrades archive. Rows are aggregated into `(price bucket, 30-minute period)` cells as they stream past and then dropped, so memory is bounded by buckets × periods rather than by trade count. **Raw ticks are never stored.**
- **Profile construction** — trades are bucketed into configurable price levels and TPO periods, reconstructing the market profile. Every parameter — bucket size, period width, IB length, archive URL — is read from the `instruments` table, not written into the code.
- **Structure detection** — point of control (with mid-range tie-breaking), 70% value area via neighbour expansion, initial balance, single prints as contiguous ranges, poor highs and lows, distribution splits, and one-timeframing across periods.
- **Classification** — a decision tree over those signals produces a day type (Non-Trend, Neutral, Double-Distribution, Trend, Directional, Normal) along with the reasoning that drove the verdict.
- **Caching** — the computed session is the expensive primitive, so it is persisted once and served from Postgres afterwards. Everything else derives from it.

## Availability and latency

Two things worth knowing before you call it:

- **Sessions publish T+1.** Binance publishes each day's archive at roughly **02:00 UTC the following day**, so the most recent session you can request is yesterday (UTC). Asking for today or any future date returns `404`, and so does a past date whose archive is not up yet.
- **A cache miss is slow.** A cached session returns in milliseconds. A miss computes the session inline — downloading and aggregating the whole day's archive — which takes **roughly 20 seconds**, against a 90-second request budget. Once computed, a session stays cached.

Requests are limited to **30 per minute per IP**, and at most **3 cold computes** run concurrently; over that you get a `503` with a `Retry-After` header rather than a queue.

## Stack

Python · FastAPI · PostgreSQL · Docker · pytest

## Running It

Build and run the container:

```bash
docker build -t market-structure-engine .
docker run --env-file .env -p 8000:8000 market-structure-engine
```

The service expects a `CONNECTION_STRING` environment variable pointing at a PostgreSQL instance.

### Endpoints

| Endpoint | Description |
|---|---|
| `GET /v1/health` | Liveness plus a storage round-trip. `503` if the database is unreachable |
| `GET /v1/instruments` | Instruments the engine is configured for, with their parameters |
| `GET /v1/sessions/{symbol}/{date}` | One computed session. Cache hit → milliseconds; miss → computes inline (~20 s) |
| `GET /v1/sessions/{symbol}?from=&to=` | Cached sessions in a window, plus `missing: [dates]`. Never triggers a compute |
| `GET /v1/sessions/{symbol}/{date}/report` | The same session rendered as the written text report |
| `GET /docs` | Auto-generated interactive API docs, fully typed |

`date` is `YYYY-MM-DD`; anything else is a `422`. An unknown or inactive symbol is a `404`. An archive that should exist but cannot be fetched is a `503`.

To fill a window, ask the range endpoint what is `missing` and then request those dates individually — the range endpoint deliberately never computes, so a wide query can never turn into an accidental hour of downloads.

## Tests

```bash
python -m pytest
```

The endpoint, engine and storage tests are pure — storage and the archive are both faked — so they need neither a database nor the network. The 8-day parity suite, which reproduces the retired SQL path exactly, downloads real archives and is skipped in CI.

## Design Notes

- Computed sessions are cached with the `engine_version` that produced them. Bumping that constant invalidates every row lazily: a stale row is recomputed and overwritten the next time it is requested, with no migration step.
- The engine never imports FastAPI. It raises its own exceptions (`SessionNotPublished`, `ArchiveUnavailable`, `NoTradeData`) and returns plain dictionaries; mapping those to status codes and Pydantic models happens only in the API layer.
- The schema is instrument-agnostic — bucket size, period width, IB length and the archive URL template are columns — so an additional market is a row rather than a code change.
- `compute_structures()` is the single source of truth feeding both the JSON response and the written report, and its return value *is* the `daily_levels` row.

**Live:** https://market-structure-engine-production.up.railway.app/docs
