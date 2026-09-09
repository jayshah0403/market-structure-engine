# Market Structure Engine — Current State (v1 inventory, Sept 2026)

Baseline document. Everything the system does today, how it does it, and the debts to spec against. No proposals in here — the v2 spec is a separate doc.

---

## 1. Components

| File / thing | Role | Status |
|---|---|---|
| `ingest.py` | Everything: DB connection, ingestion (2 paths), profile computation, all detectors, classifier, report generator | Working; ~300 lines, single module |
| `api.py` | FastAPI wrapper — 2 endpoints + auto `/docs` | Working code; serves nothing (DB dead) |
| `tests/test_engine.py` + `conftest.py` | 3 pure unit tests + 1 DB integration test | Pure tests pass without DB (lazy connection) |
| `Dockerfile`, `.dockerignore`, `requirements.txt` | `python:3.13-slim`, deps: fastapi, uvicorn, psycopg2-binary, requests, python-dotenv; `.env` excluded from image | Working |
| Railway | Hosts the container; `CONNECTION_STRING` injected as env var | **Paid plan active (Sept 2026); service was offline after trial expiry — needs redeploy** |
| Supabase (Postgres, free tier, t4g.nano, ap-southeast-2) | The only data store | **DEAD** — disk full → unrecoverable WAL recovery loop. Must be recreated. |
| `README.md` | Pitch + endpoints + setup | Links the Railway URL |

---

## 2. Data layer (schema as it existed)

**`instruments`** — `id` (PK), `symbol` (e.g. `BTCUSDT`), plus per-instrument config columns (bucket size / tick size) that were added but **never read by the SQL** — the queries hardcode `25`.

**`trades`** — `agg_trade_id` (PK, dedup key), `instrument_id` (FK → instruments), `price NUMERIC`, `quantity NUMERIC`, `ts TIMESTAMPTZ`, `is_buyer_maker BOOL`. One row per Binance aggTrade. ~760k–5M rows/day for BTC.

**`staging_trades`** — 8 raw columns mirroring the Binance archive CSV exactly (`agg_trade_id, price, quantity, first_id, last_id, ts_micro BIGINT, is_buyer_maker, is_best_match`). No constraints. Truncated after each day's load.

**Storage model:** raw ticks retained forever. This is what killed the free tier (16 days ≈ tens of millions of rows + PK index + WAL).

---

## 3. Ingestion — two paths

**3a. `fetchDayRecords(date_ms)` — live REST API path (legacy, slow)**
- Looks up `instrument_id` for hardcoded `"BTCUSDT"`.
- Finds the first aggTrade at/after `date_ms` via `startTime`, then pages forward by `fromId` in batches of 1000 until `T > date_ms + 86400000`.
- Filters each batch to `T <= end_time`, inserts via `execute_values` with `ON CONFLICT (agg_trade_id) DO NOTHING`, commits per batch.
- Timestamps from the REST API are **milliseconds** → `to_timestamp(%s / 1000.0)`.
- ~20 min/day. Reconnect/resume works because of `ON CONFLICT`.

**3b. `load_day_from_archive(date_str)` — bulk archive path (current, fast)**

> **Not in the repo.** Written on the old machine in August and never pushed; the machine is inaccessible. The design is recorded here for the story; PR 2 removes this path regardless.
- URL: `https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-{date}.zip`
- `requests.get` → `io.BytesIO` → `zipfile.ZipFile` → single headerless CSV inside.
- `cur.copy_expert("COPY staging_trades FROM STDIN WITH (FORMAT csv)", f)` — streams the CSV server-side.
- `INSERT INTO trades (...) SELECT agg_trade_id, (SELECT id FROM instruments WHERE symbol='BTCUSDT'), price, quantity, to_timestamp(ts_micro / 1000000.0), is_buyer_maker FROM staging_trades ON CONFLICT DO NOTHING;`
- Archive timestamps are **microseconds** (16 digits) — hence `/ 1000000.0`. (REST path is ms; the two paths use different divisors — a trap.)
- `TRUNCATE staging_trades;` then `commit`.
- Requires `SET statement_timeout = '600000'` on the session or Supabase kills the COPY (default timeout too short).
- ~5 min/day on free tier. Verified exact: July 27 2026 → 762,859 rows = CSV line count.
- Symbol, URL, and instrument lookup all hardcoded to BTCUSDT.

**3c. Backfill loop** — `for i in range(20): load_day_from_archive(start + timedelta(days=i))` with `try/except` printing failures. **Known bug:** the `except` does not `conn.rollback()`, so one failed day (e.g. transient "read-only transaction") poisons the connection and every subsequent day fails with "current transaction is aborted". Fix written (`if conn: conn.rollback()`), never run. Days 13–16 July loaded before the cascade; DB died before the rerun.

**3d. Connection** — lazy: module-level `conn = None; cur = None`; `get_cursor()` connects on first call from `os.environ["CONNECTION_STRING"]` and reuses. Importing `ingest` has no side effects (that's what lets pure tests run without a DB). Report call is under `if __name__ == "__main__":`.

---

## 4. Analysis pipeline (per UTC calendar day, single instrument)

Session = UTC 00:00–24:00. Bucket = $25. Period = 30 min (48 periods, lettered A–Z then a–v).

1. **`get_profile_grid(start_ts_ms)`** — one SQL GROUP BY over `trades`:
   `FLOOR(price/25)*25` × `FLOOR(EXTRACT(EPOCH FROM (ts AT TIME ZONE 'UTC')::time)/1800)` → `COUNT(*)`, `SUM(quantity)`. Half-open window `ts >= start AND ts < start+1d`.
2. **`compute_profile`** → `{price_bucket: [periods present]}` (the TPO profile; trade_count/volume are fetched but unused).
3. **`compute_poc(levels, counts)`** — bucket with max TPO count; tie-break = closest to range midpoint `(high+low)/2`.
4. **`compute_value_area(poc, levels, counts)`** — expand outward from POC one bucket at a time, taking whichever neighbour (above/below) has the larger count, until captured ≥ 70% of total TPOs. Returns `(vah, val)`.
5. **`compute_ib(profile)`** — IB = all buckets touched in periods 0 or 1 (first hour UTC) → `(ib_high, ib_low)`. Same pass also collects `arr_single_tpo` = buckets with exactly one TPO (single prints, as a raw list of bucket prices, anywhere in the profile).
6. **`segment_profile(levels, counts, thin_max=1)`** — walk buckets top-down into runs: `thick` (count ≥ 2) / `thin` (count ≤ 1). Returns `[["thick",[...]],["thin",[...]],...]`.
7. **`detect_double_distribution_split(segments, min_thick_levels=3)`** — True if any `thick(≥3 levels) → thin → thick(≥3 levels)` sequence exists. Verified against synthetic tail-vs-DD cases (unit tests).
8. **`detect_trend(start_ts)`** — second SQL: per-period `MAX(price), MIN(price)`. Counts violations: uptrend violated when a period's low < previous period's low; downtrend violated when a period's high > previous high. Returns `(1 − up_viol/(n−1), 1 − down_viol/(n−1))` = one-timeframing confidence. Raises `ValueError` if < 2 periods (guard against the divide-by-negative-one fabrication bug).
9. **`classify_day_type(...)`** — extension multiples `ext_up = (day_high − ib_high)/ib_range`, `ext_down = (ib_low − day_low)/ib_range`. Thresholds `SMALL=0.4, MEANINGFUL=0.6, TREND_THRESH=0.8`. Decision order:
   Non-Trend (both < 0.4) → Neutral (both > 0.6) → Double-Distribution Trend (is_dd) → Trend down (down_conf > 0.8) → Trend up (up_conf > 0.8) → Directional (unclassified) (either side > 0.6) → Normal. Returns `(label, reason_string)`.
10. **`compute_structures(start_ts)`** — orchestrates 1–9, raises `ValueError` if the profile is empty, returns a JSON-clean dict:
    `date` (YYYY-MM-DD UTC), `day_type`, `reason`, `poc`, `vah`, `val`, `ib_high`, `ib_low`, `arr_single_tpo` (list), `poor_high`, `poor_low`, `extension_above`, `extension_below`, `buying_tail`, `selling_tail`, `day_low`, `day_high`.
11. **`generate_report(structures)`** — fixed-format text block (day type, reason, POC, VA, IB, extension, tails, poor high/low).

**Validation history:** POC/VA/IB matched Exocharts within 1–2 buckets on July 7 2026; DD detector verified on synthetic cases; classifier output on July 7 = "Directional (unclassified), extended 4.1x IB down; one-timeframing 0.60".

---

## 5. API surface (`api.py`)

| Endpoint | Input | Output | Errors |
|---|---|---|---|
| `GET /profile/{start_ts}` | `start_ts: int` — **UTC midnight in epoch milliseconds** (e.g. `1783382400000`) | `compute_structures` dict as JSON | `ValueError` → `404 {"detail": ...}` |
| `GET /report/{start_ts}` | same | `{"report": "<text block>"}` | same |
| `GET /docs` | — | auto OpenAPI UI | — |

- No Pydantic response models → `/docs` shows untyped responses.
- No instrument parameter — BTCUSDT everywhere.
- No listing, no range, no "what days exist", no cross-day endpoint.
- No caching: every request recomputes both SQL aggregations over millions of rows.
- No auth, no rate limiting, no `/v1` versioning, no health endpoint, no request logging.
- Non-midnight or non-UTC timestamps silently produce a partial/shifted window (no validation that `start_ts` is a day boundary).

---

## 6. Tests (`tests/test_engine.py`)

- `test_tail_is_not_double_distribution` — synthetic single-distribution + long tail → False.
- `test_genuine_double_distribution` — synthetic thick–thin–thick → True.
- `test_classifies_directional_when_one_sided_no_trend` — classifier branch check.
- `test_raises_on_missing_data` — integration; needs DB; asserts `ValueError` on an un-ingested date.
- Run: `python -m pytest -v`. Pure tests pass with no `.env` (verified). No CI.

---

## 7. Known bugs / debts (the list the v2 spec should address or consciously defer)

**Correctness — verify before anything else**
- Backfill loop lacks `conn.rollback()` in the `except` (cascade failure). **Deferred, not fixed:** the loop and `load_day_from_archive` are not in the repo (§3b), so PR 1 had nothing to patch. The requirement moved to PR 7 (Scheduled warm-up), which is where backfill is re-implemented.

**Architecture**
- Raw ticks retained forever; engine only ever reads aggregates. Root cause of the outage. Decided direction: compute per-day levels on ingest → persist `daily_levels` → discard ticks.
- Single ~300-line module holds ingestion, computation, and rendering.
- Instrument config columns exist but bucket size (25), period (1800s), symbol, archive URL, and IB definition (periods 0–1) are all hardcoded.
- Session hardwired to UTC calendar day — no session-template concept.
- REST path (ms) and archive path (µs) are two divergent code paths for the same table.
- `arr_single_tpo` is a flat list of bucket prices, not contiguous ranges — a consumer can't tell one single-print zone from three.

**API**
- Millisecond-epoch path parameter instead of dates; no instrument in the route; no listing/range/cross-day; no typed schemas; no caching; recomputes on every hit.

**Ops**
- Supabase project must be recreated from scratch (schema + tables). Railway service must be redeployed with the new `CONNECTION_STRING`. README URL/status may be stale.
- No scheduled ingestion — data only exists for days manually loaded.
- No linting, no dependency pinning. (CI added in PR 1: GitHub Actions runs `python -m pytest` on push and pull request.)

---

## 8. What is genuinely done and should be preserved

- The detector logic (POC tie-break, 70% VA expansion, IB, DD split with synthetic verification, one-timeframing confidence, classifier decision tree with reasons) — validated, keep.
- The COPY → staging → INSERT…SELECT bulk-load pattern — correct, keep.
- Lazy connection + `__main__` guard — keep.
- `compute_structures` as the single source of truth feeding both JSON and text — keep the shape; it becomes the `daily_levels` row.
- Dockerfile / Railway deploy path — keep.
