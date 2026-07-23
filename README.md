# Market Structure Engine

An API that builds the market profile of an instrument (currently BTC/USDT), identifies the key levels of that profile, classifies the day type, and returns it all as JSON.

## Sample Output

```
════════════════════════════════════════
BTC/USDT — Session Report, 2026-07-07
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

The same session as JSON, from `GET /profile/{start_ts_ms}`:

```json
{
  "date": "2026-07-07",
  "day_type": "Directional (unclassified)",
  "reason": "Extended 4.1x IB to the down; one-timeframing 0.60 — directional but not a clean trend",
  "poc": 63250.0,
  "vah": 63825.0,
  "val": 63050.0,
  "ib_high": 64300.0,
  "ib_low": 63975.0,
  "poor_high": false,
  "poor_low": false,
  "extension_above": 0.0,
  "extension_below": 4.08
}
```

## Why

I built this because there are currently no market profile APIs offering these services. If someone is building a trading bot and wants to trade around the key levels a market profile marks out, they can identify those levels with a single API call. And since Exocharts — the actual software — is expensive, this gives an alternative way to access the same data.

## How It Works

The pipeline, end to end:

- **Ingestion** — paginated pulls from the Binance aggTrades endpoint into PostgreSQL, using an ID cursor, batch inserts, idempotent deduplication, and connection-drop recovery. Roughly 830k trades for a full BTC session.
- **Profile construction** — trades are bucketed into configurable price levels and 30-minute TPO periods, reconstructing the market profile from raw tick data.
- **Structure detection** — point of control (with mid-range tie-breaking), 70% value area via neighbour expansion, initial balance, single prints, poor highs and lows, distribution splits, and one-timeframing across periods.
- **Classification** — a decision tree over those signals produces a day type (Non-Trend, Neutral, Double-Distribution, Trend, Directional, Normal) along with the reasoning that drove the verdict.
- **Reporting** — the computed structures are rendered as a written session report, and served over HTTP alongside the raw JSON.

## Verification

Outputs were validated against Exocharts for the same session. Point of control, value area, and overall profile shape matched within one price bucket.

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
| `GET /profile/{start_ts_ms}` | Market profile structures as JSON |
| `GET /report/{start_ts_ms}` | Formatted written session report |
| `GET /docs` | Auto-generated interactive API docs |

`start_ts_ms` is the session start as a Unix timestamp in milliseconds (sessions run 00:00–24:00 UTC). Requesting a date with no ingested data returns a `404` rather than a fabricated result.

## Tests

```bash
python -m pytest
```

Covers distribution-split detection against synthetic profiles (including the tail-vs-split case), day-type classification, and the empty-data guard.

## Design Notes

- Prices are stored as `NUMERIC` and handled as exact decimals through the pipeline to avoid floating-point drift at price levels.
- The schema is instrument-agnostic, with per-instrument session boundaries and tick size held as configuration, so additional markets are a row rather than a code change.
- Structure computation is separated from rendering: a single `compute_structures()` call produces the dictionary that serves both the JSON response and the written report.

**Live:** https://market-structure-engine-production.up.railway.app/doc
