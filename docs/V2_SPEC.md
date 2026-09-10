# Market Structure Engine — V2 Spec

Companion to `docs/CURRENT_STATE.md`. This document is the contract for v2. Each PR section has four parts: **what**, **interface**, **rules**, **acceptance**. A PR merges only when its acceptance tests pass and the owner can explain every line of the diff.

Items marked **[DECIDE]** are open parameters the owner must set before the relevant PR starts. Defaults are proposed inline.

---

## 0. Goals and non-goals

**Goal.** A public, usable HTTP API that returns computed market-structure facts for any completed session of a supported instrument — daily levels, level history (naked POCs, poor highs/lows), composites, and confluence-ranked zones — without storing raw ticks.

**Non-goals (v2).** No trade signals, entries, exits, or direction calls — the engine locates structure only. No intraday / current-day data (archive publishes T+1). No authentication (rate limiting only). No instruments beyond the seeded list. No UI.

**Guiding principle.** Cache the expensive primitive (a day's computed profile); derive everything else from cached primitives. Raw ticks are transient and never persisted.

---

## 1. Architecture

```
GET /v1/... ──► API (FastAPI, Railway)
                  │
                  ├─ cache hit ──► Postgres (Supabase free tier): daily_levels, composites  ──► response (ms)
                  │
                  └─ cache miss ─► compute_day(symbol, date):
                                    download data.binance.vision zip
                                    stream CSV → (bucket, period) presence + period hi/lo   [memory: O(buckets×periods)]
                                    run detectors (unchanged)
                                    INSERT daily_levels row
                                    update composite membership
                                   ──► response (~10–30 s cold)

Scheduler (daily, after archive publish) ──► compute_day(yesterday) for each instrument ──► keeps rolling window warm
```

Persistent storage is two small tables. `trades` and `staging_trades` are removed from the codebase.

---

## 2. Data model

### 2.1 `instruments`
| column | type | notes |
|---|---|---|
| symbol | TEXT PK | e.g. `BTCUSDT` |
| exchange | TEXT | `binance-spot` |
| bucket_size | NUMERIC | `25` for BTCUSDT — **read by the engine, not hardcoded** |
| period_seconds | INT | `1800` |
| ib_periods | INT | `2` (first hour) |
| archive_url_template | TEXT | `https://data.binance.vision/data/spot/daily/aggTrades/{symbol}/{symbol}-aggTrades-{date}.zip` |
| active | BOOL | |

Seed: BTCUSDT only. **[DECIDE]** any second instrument at launch (ETHUSDT?) — default: no.

### 2.2 `daily_levels` — one row per (symbol, session_date)
| column | type | notes |
|---|---|---|
| symbol | TEXT FK | |
| session_date | DATE | UTC calendar day |
| engine_version | INT | bump when any detector changes → stale rows recompute lazily |
| computed_at | TIMESTAMPTZ | |
| day_type, reason | TEXT | from classifier |
| poc, vah, val | NUMERIC | |
| ib_high, ib_low | NUMERIC | |
| day_high, day_low | NUMERIC | |
| poor_high, poor_low | NUMERIC NULL | **poor_high derives from day_high; poor_low from day_low** (fixes the v1 swap) |
| buying_tail, selling_tail | NUMERIC NULL | |
| extension_above, extension_below | NUMERIC | multiples of IB range |
| up_conf, down_conf | NUMERIC | one-timeframing confidences |
| single_prints | JSONB | list of contiguous ranges `[{"from":63000,"to":63075}, …]` — **ranges, not a flat bucket list** |
| profile | JSONB | `{"63225": 19, "63250": 18, …}` bucket → TPO count. Required for composites. |
| composite_id | INT NULL FK | NULL = not part of any ≥2-day composite |
| PK | (symbol, session_date) | |

### 2.3 `composites` — one row per emitted composite
| column | type | notes |
|---|---|---|
| id | SERIAL PK | |
| symbol | TEXT FK | |
| start_date, end_date | DATE | inclusive |
| days | INT | ≥ 2 |
| status | TEXT | `open` (still accepting days) / `closed` |
| profile | JSONB | merged histogram (sum of member `profile`s) |
| poc, vah, val, high, low | NUMERIC | computed from merged histogram with the same detectors |
| single_prints | JSONB | ranges, from merged histogram |
| poor_high, poor_low | NUMERIC NULL | |
| engine_version | INT | |

---

## 3. PR sequence

### PR 1 — Correctness fixes on v1 (no new features)

**What.** Fix known bugs before any feature inherits them. Add CI.

**Rules.**
- `poor_high = day_high if tpo_count(day_high) >= 2 else None`; `poor_low = day_low if tpo_count(day_low) >= 2 else None`.
- Tails: if no single-print buckets exist, `buying_tail = selling_tail = None`; never call `min()`/`max()` on a possibly-empty list.
- Add GitHub Actions workflow running `pytest` on push/PR.

*(The backfill-rollback fix formerly specified here moved to PR 7: the v1 backfill loop is not in the repo — see CURRENT_STATE §3b — and PR 7 is where backfill is re-implemented.)*

**Acceptance.**
- Unit test: synthetic profile with ≥2 TPOs at the high and 1 at the low → `poor_high == day_high`, `poor_low is None`.
- Unit test: profile with zero single-print buckets → tails are `None`, no exception.
- CI green.

---

### PR 2 — New data layer

**What.** ~~Fresh Supabase project.~~ **Reuse the existing Supabase project** (owner decision at PR 2: it is reachable again, so it was re-schemad in place rather than recreated; `db/schema.sql` drops the v1 tick tables at the top, which is what relieves the disk pressure). Create `instruments`, `daily_levels`, `composites` (DDL in `db/schema.sql`, committed). Seed BTCUSDT. Remove `trades`, `staging_trades`, `fetchDayRecords`, `load_day_from_archive`, and the COPY/staging code from the codebase (they live in git history).

**Interface.** `db.py` module: `get_connection()` (lazy, from `CONNECTION_STRING`), `get_daily_levels(symbol, date)`, `upsert_daily_levels(row)`, `list_daily_levels(symbol, from_date, to_date)` (`from` is a Python keyword — the signature reads `from_date`/`to_date`, inclusive), `get_instrument(symbol)`. `get_cursor()` moves here from `ingest.py`. Reads return plain dicts with NUMERIC as `float`, JSONB parsed, dates as `date`.

**Rules.**
- Schema exactly as §2. Upsert keyed on `(symbol, session_date)`.
- Nothing in this PR downloads or computes; it is storage only.

**Acceptance.**
- `schema.sql` applies cleanly to an empty database.
- Round-trip test: upsert a fixture row → read it back equal (including JSONB fields).
- No reference to `staging_trades` or the COPY/staging path anywhere in the repo, and no code that writes ticks. **Two `FROM trades` queries remain, in `ingest.py`'s `get_profile_grid` and `detect_trend`:** PR 3 replaces both with the in-memory archive path, and deleting them in PR 2 would mean deleting `compute_structures`, `api.py`'s two routes and the PR 1 acceptance tests a PR early. They are documented as dead-until-PR-3 in the `ingest.py` docstring. `scripts/capture_v1_golden.py` also names `trades` by necessity — it is the one-shot that read the table before it was dropped.
- Railway `CONNECTION_STRING` needs no change (same project reused, same credentials); the redeploy Railway already needed is still outstanding and `/health` (added in PR 4) will verify it.

---

### PR 3 — In-memory compute path

**What.** `compute_day(symbol, session_date) -> DailyLevels` — archive → streaming aggregation → detectors → row. Replaces the Postgres-backed profile queries.

**Interface.**
```python
def compute_day(symbol: str, session_date: date) -> DailyLevels:   # pure compute + persist
def aggregate_archive(csv_stream, bucket_size, period_seconds) -> tuple[dict[float, set[int]], dict[int, tuple[float, float]]]
    # returns profile {bucket: set(periods)} and period_ranges {period: (high, low)}
```

**Rules.**
- Download `archive_url_template` for the symbol/date. On HTTP 404 → raise `SessionNotPublished` (archive appears ~02:00 UTC next day).
- Parse CSV **in chunks** (`csv` module over a `TextIOWrapper`, or `pandas.read_csv(chunksize=…)`). Never load the full file into one DataFrame. Accumulator state is bounded by buckets × periods.
- Column positions (headerless): `[0]=agg_trade_id, [1]=price, [2]=qty, [5]=timestamp_micro, [6]=is_buyer_maker`. Timestamp is **microseconds**.
- `bucket = floor(price / bucket_size) * bucket_size`; `period = floor(seconds_since_utc_midnight / period_seconds)`.
- Existing detectors (`compute_poc`, `compute_value_area`, `compute_ib`, `segment_profile`, `detect_double_distribution_split`, `classify_day_type`) are called **unchanged** on the in-memory profile. `detect_trend` is refactored to take `period_ranges` instead of running SQL.
- `single_prints` are emitted as contiguous ranges.
- Result is upserted into `daily_levels` with the current `ENGINE_VERSION`.
- Memory ceiling **[DECIDE]** — default: process must stay < 256 MB on a 5M-row day (assert in an integration test with a fixture-sized file).

**Acceptance.**
- Golden test: a committed fixture CSV (a small synthetic day) → known POC/VAH/VAL/IB.
- Regression test: `compute_day("BTCUSDT", 2026-07-27)` reproduces the v1 Postgres-path values recorded in CURRENT_STATE (POC/VA/IB within one bucket).
- Unit test: `detect_trend(period_ranges)` gives identical output to the old SQL version on a fixture.
- Unit test: unpublished date → `SessionNotPublished`, no partial row written.

---

### PR 4 — Cache-then-compute API

**What.** Replace `/profile/{ms}` and `/report/{ms}` with a versioned, typed, date-based API.

**Interface.**
| Method / route | Behaviour |
|---|---|
| `GET /v1/health` | `{status, db: ok/fail, engine_version}` |
| `GET /v1/instruments` | seeded instruments |
| `GET /v1/sessions/{symbol}/{date}` | cache hit → row; miss → `compute_day` synchronously → row. **[DECIDE]** sync (default, request timeout 90 s) vs async `202 + job id`. |
| `GET /v1/sessions/{symbol}?from=&to=` | cached rows in range + `missing: [dates]`. Does **not** trigger compute. |
| `GET /v1/sessions/{symbol}/{date}/report` | text report (v1 `generate_report`) |

All responses are Pydantic models (`DailyLevels`, `SessionList`, …) so `/docs` is fully typed.

**Rules.**
- `date` is `YYYY-MM-DD`; anything else → 422. Unknown/inactive symbol → 404. Date ≥ today (UTC) → 404 `session not yet published`. Archive download failure → 503.
- Cache hit requires `engine_version == ENGINE_VERSION`; stale rows recompute and overwrite.
- Rate limit **[DECIDE]** — default 30 req/min per IP (`slowapi`); cold computes additionally limited to **[DECIDE]** default 3 concurrent.
- Old `/profile` and `/report` routes removed. README updated.

**Acceptance.**
- Endpoint tests with a mocked `compute_day`: hit path returns cached row without calling compute; miss path calls it once and returns the persisted row.
- Validation tests: bad date → 422, future date → 404, bad symbol → 404.
- `/docs` renders typed schemas (snapshot test of the OpenAPI JSON).
- Deployed on Railway with the new DB; `/v1/health` returns `db: ok`.

---

### PR 5 — Level history (naked POCs, poor extremes)

**What.** Track whether prior levels have been traded through, and expose the unfilled ones.

**Interface.**
```python
def traded_through(symbol, level: float, since: date, until: date) -> date | None
    # first session_date in (since, until] whose [day_low, day_high] contains level; None if never
```
| route | returns |
|---|---|
| `GET /v1/levels/{symbol}/naked-pocs?as_of=&lookback=30` | POCs of sessions in the lookback not traded through by any later session ≤ as_of |
| `GET /v1/levels/{symbol}/poor-extremes?as_of=&lookback=30` | poor highs/lows not yet traded through |

**Rules.**
- "Traded through" **[DECIDE]** — default: the level lies within a later day's `[day_low, day_high]` (touched). Alternative: require a later day's *close/POC* beyond the level. Pick one and state it.
- Computed on the fly from `daily_levels` (cheap). Lookback default 30, max **[DECIDE]** 90.
- If any date in the lookback is missing from the cache → response includes `missing: [dates]` and results are marked `partial: true`; never silently compute over gaps.

**Acceptance.**
- Unit tests with fixture rows: POC filled on day+2 → not naked as_of day+2, naked as_of day+1.
- Gap handling test: missing day → `partial: true`.

---

### PR 6 — Composites

**What.** Segment the session timeline into composites and compute composite-level structures.

**Interface.**

Segmentation is **not** performed per request. It runs once per day at ingest (PR 3 hook) and persists `composites` rows. The endpoints only read those rows.

| route | returns |
|---|---|
| `GET /v1/composites/{symbol}?from=&to=&as_of=` | all persisted composites whose `[start_date, end_date]` **intersects** `[from, to]`, returned whole (not clipped to the range), sorted by `priority` desc. `as_of` defaults to `to`. |
| `GET /v1/composites/{symbol}/current?as_of=` | the open composite containing `as_of` (404 if `as_of` is unassigned) |

**The range endpoint never merges the days in the range.** It filters pre-segmented composites. A day inside the range that belongs to no composite simply does not appear.

Worked example — cached days Aug 1–20; ingest-time walk produced C1 = Aug 1–5, Aug 6 unassigned, C2 = Aug 7–14 (closed), C3 = Aug 15–20 (open). Request `from=2026-08-10&to=2026-08-20`:

```json
{
  "symbol": "BTCUSDT", "from": "2026-08-10", "to": "2026-08-20", "as_of": "2026-08-20",
  "partial": false, "missing": [],
  "composites": [
    { "id": 3, "status": "open",   "start_date": "2026-08-15", "end_date": "2026-08-20", "days": 6,
      "priority": 6.0,  "poc": 63250, "vah": 63825, "val": 63050, "high": 64300, "low": 62650,
      "naked_poc": true, "poor_high": null, "poor_low": 62650, "single_prints": [{"from": 64200, "to": 64300}] },
    { "id": 2, "status": "closed", "start_date": "2026-08-07", "end_date": "2026-08-14", "days": 8,
      "priority": 6.67, "poc": 61900, "vah": 62400, "val": 61500, "high": 62900, "low": 61100,
      "naked_poc": false, "poor_high": 62900, "poor_low": null, "single_prints": [] }
  ]
}
```
C1 is excluded (ends before `from`); C2 is returned whole although it starts before `from`; Aug 6 appears in nothing.

**Priority (ranking).** `priority = days / (1 + age_days / RECENCY_HALF)` where `age_days = as_of − end_date` (0 for the open composite) and `RECENCY_HALF` **[DECIDE D12]** default 30. Longer composites score higher; older ones decay; the open composite is never penalized. Response is sorted by `priority` desc; both `days` and `priority` are returned so clients can re-rank.

**Gaps.** Segmentation is defined only over contiguous cached days. If any date in `[from, to]` is not cached, respond with `partial: true` and `missing: [dates]`; never segment across a gap.

**Rules (verbatim — do not reinterpret).**
> Walk days chronologically from the earliest cached day. Day *N* joins the open composite iff `overlap(VA_N, VA_composite) / (VAH_N − VAL_N) ≥ 0.5`, where `overlap` is the length of the intersection of the two value-area ranges and `VA_composite` is the value area of the composite's **merged histogram** (all member `profile` maps summed, then `compute_value_area` applied). After each merge, re-sum and recompute `VA_composite` before evaluating the next day. For a candidate with one member, `VA_composite` is that day's VA. If the condition fails, the open composite closes and day *N* opens a new candidate. Comparison is never against older composites and never backward. A composite is emitted only if it contains ≥ 2 days; a candidate that never gains a second day leaves its day with `composite_id = NULL`. Assignment is forward-only and never revised.

- `VA_composite` is **never** the envelope `[min(VAL), max(VAH)]`.
- Composite-level structures (POC, VAH, VAL, high, low, single-print ranges, poor high/low) are computed from the merged histogram with the unchanged detectors. Composite naked POC = composite POC not traded through by any session after `end_date` (uses `traded_through`).
- Membership is assigned incrementally when a day is computed (PR 3 hook) so `composite_id` is persisted; a full re-walk is available as a maintenance command and must produce identical assignments.
- Threshold `0.5` and min days `2` are constants; not user-overridable in v2.

**Acceptance.**
- Fixture timeline tests: (a) three overlapping days → one composite of 3; (b) two overlapping, one migrating, two overlapping → two composites, migrating day `NULL`; (c) slow drift where each day overlaps the *previous day* but not the merged VA → composite closes (proves cumulative rule, not chain rule).
- Envelope test: a case where envelope-VA would merge and merged-histogram-VA would not → must not merge.
- Determinism: incremental assignment == full re-walk on a 30-day fixture.

---

### PR 7 — Scheduled warm-up

**What.** Keep a rolling window computed without user traffic.

**Rules.**
- Mechanism **[DECIDE]** — default: Railway cron service hitting an internal `POST /v1/admin/compute?date=yesterday` protected by a token; alternative: in-process APScheduler.
- Runs daily at **[DECIDE]** default 03:00 UTC; computes T−1 for every active instrument; retries once after 1 h on `SessionNotPublished`.
- Window **[DECIDE]** default 30 days; on first deploy, backfills the window (sequentially, respecting the concurrency limit).
- Backfill iterates days sequentially; on any failure it rolls back the DB transaction, logs the date, and continues to the next day. One failed day must never poison subsequent days.

**Acceptance.**
- Job is idempotent (re-running for a computed date is a no-op).
- Backfill test on a 5-day fixture window.
- Injected failure on day 2 of a 5-day fixture → day 2 logged, days 3–5 still computed, `rollback()` called exactly once.

---

### PR 8 — Confluence zones

**What.** Cluster all live levels into ranked price zones — the "where are the key areas" endpoint.

**Interface.** `GET /v1/confluence/{symbol}?as_of=&lookback=30` → `[{zone_low, zone_high, score, levels:[{type, source_date|composite_id, price}]}]` sorted by score desc.

**Rules.**
- Inputs: naked daily POCs, unfilled poor highs/lows, daily single-print range edges, prior session VAH/VAL/POC, open-composite VAH/VAL/POC, composite naked POCs, closed-composite unfilled poor extremes.
- Clustering: levels within **[DECIDE]** default 2 buckets of each other join a zone (single-linkage).
- Score = sum of level weights **[DECIDE]** — owner supplies the weight table (e.g. composite POC 3, naked daily POC 2, poor extreme 2, VA edge 1, single-print edge 1). Recency multiplier **[DECIDE]** default none in v2.
- Output is descriptive only. No direction, bias, entry, or target fields — ever.

**Acceptance.**
- Fixture test: two levels 1 bucket apart form one zone; 5 buckets apart form two.
- Score ordering test with the weight table.
- Schema test asserts the response has no directional fields.

---

## 4. Cross-cutting

- **Versioning:** `ENGINE_VERSION` constant; bump on any detector change; stale cache recomputes lazily.
- **Errors:** custom exceptions in the engine (`SessionNotPublished`, `NoTradeData`, `ArchiveUnavailable`); translated to HTTP only in the API layer. The engine never imports FastAPI.
- **Config:** all per-instrument parameters come from `instruments`; no hardcoded symbol, bucket, period, or URL anywhere in engine code.
- **Tests:** pure engine tests need no network or DB; archive downloads are mocked with committed fixtures. CI runs on every PR.
- **Docs:** README rewritten for v2 routes; `CURRENT_STATE.md` updated at the end of each PR.

## 5. Definition of done (every PR)
1. Acceptance tests in this doc pass in CI.
2. `CURRENT_STATE.md` reflects the change.
3. The owner has read the full diff and can explain every hunk. If not, the PR is not merged — ask the agent to explain, then re-review.

## 6. Open decisions summary
| # | decision | default |
|---|---|---|
| D1 | second instrument at launch | no |
| D2 | compute memory ceiling | 256 MB |
| D3 | cold path sync vs async | sync, 90 s |
| D4 | rate limit | 30/min/IP; 3 concurrent cold computes |
| D5 | traded-through definition | touched (within later day's range) |
| D6 | max lookback | 90 |
| D7 | scheduler mechanism | Railway cron + admin endpoint |
| D8 | scheduler time / window | 03:00 UTC / 30 days |
| D9 | confluence cluster tolerance | 2 buckets |
| D10 | confluence weight table | owner supplies |
| D11 | confluence recency multiplier | none in v2 |
| D12 | composite priority `RECENCY_HALF` (days) | 30 |