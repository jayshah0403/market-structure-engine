# Market Structure Engine — Current State (Sept 2026; v1 inventory + PRs 1–2)

Baseline document. Everything the system does today, how it does it, and the debts to spec against. No proposals in here — the v2 spec is a separate doc.

---

## 1. Components

| File / thing | Role | Status |
|---|---|---|
| `ingest.py` | Profile computation, all detectors, classifier, report generator. No DB connection code and no ingestion since PR 2 | ~240 lines; `get_profile_grid` and `detect_trend` still query the dropped `trades` table — replaced in PR 3 |
| `db.py` | The only module holding SQL against the v2 tables: lazy `get_connection`/`get_cursor`, `get_instrument`, `get_daily_levels`, `upsert_daily_levels`, `list_daily_levels` | Added in PR 2; storage only, computes nothing |
| `sql/schema.sql` | v2 DDL (§2) + BTCUSDT seed. Constructive only and idempotent (`CREATE TABLE IF NOT EXISTS`, seed `ON CONFLICT DO NOTHING`), so re-applying it cannot lose a computed row | Added in PR 2 and applied |
| `sql/001_drop_v1.sql` | The v1 teardown: `DROP TABLE IF EXISTS trades, staging_trades, instruments`. Kept apart from `schema.sql` so destructive and constructive statements never share a script | **Already applied (2026-09-10) — not to be re-run.** Nothing left to drop |
| `scripts/capture_v1_golden.py` | One-shot: ran the v1 SQL path over every ingested day into `tests/fixtures/v1_golden/*.json` before the drop | Ran once; cannot be re-run (its table is gone). PR 3's regression baseline |
| `api.py` | FastAPI wrapper — 2 endpoints + auto `/docs` | Untouched by PR 2; serves nothing — both routes call `compute_structures`, whose SQL targets the dropped `trades`. Replaced in PR 4 |
| `tests/test_engine.py`, `tests/test_db.py` | 5 pure engine tests + 2 pure validation tests + 7 DB integration tests. There is no `conftest.py` (this row previously claimed one) | 14 pass with a database; 7 pass / 7 skip without one |
| `Dockerfile`, `.dockerignore`, `requirements.txt` | `python:3.13-slim`, deps: fastapi, uvicorn, psycopg2-binary, requests, python-dotenv; `.env` excluded from image | Working |
| Railway | Hosts the container; `CONNECTION_STRING` injected as env var | **Paid plan active (Sept 2026); service was offline after trial expiry — needs redeploy** |
| Supabase (Postgres 17.6, free tier, t4g.nano, ap-southeast-2) | The only data store | **LIVE** — the same project, reachable again (owner decision at PR 2: reuse, do not recreate). `sql/001_drop_v1.sql` dropped the tick tables and `sql/schema.sql` created the v2 schema: 855 MB → 10 MB, so the disk pressure that killed it is gone. `CONNECTION_STRING` is unchanged. |
| `README.md` | Pitch + endpoints + setup | Links the Railway URL |

---

## 2. Data layer (v2 schema, live since PR 2)

DDL lives in `sql/` (committed, applied). Two files, deliberately: **`sql/001_drop_v1.sql`** is the one-time v1 teardown (already applied, never to be re-run), and **`sql/schema.sql`** is constructive only and idempotent — `CREATE TABLE IF NOT EXISTS` throughout and a seed that does nothing on conflict, so re-applying it over a live database creates what is missing and cannot destroy a computed row. It is not a migration tool: changing the shape of an existing table needs its own numbered script. All runtime access goes through `db.py`.

**`instruments`** — `symbol TEXT PK`, `exchange`, `bucket_size NUMERIC`, `period_seconds INT`, `ib_periods INT`, `archive_url_template TEXT`, `active BOOL`. The v1 surrogate `id` and `tick_size`/`session_start_utc` columns are gone; `symbol` is the key everything else references. Seeded with one row: BTCUSDT / `binance-spot` / 25 / 1800 / 2 / the data.binance.vision daily aggTrades template. These columns exist so the engine can read them — PR 3 is where it starts to (the two surviving SQL queries in `ingest.py` still hardcode 25 and 1800).

**`daily_levels`** — one row per session, PK `(symbol, session_date)`; `symbol` is a FK to `instruments`. Carries `engine_version` + `computed_at` (staleness), the classifier output (`day_type`, `reason`), the levels (`poc`, `vah`, `val`, `ib_high`, `ib_low`, `day_high`, `day_low`), the nullable extras (`poor_high`, `poor_low`, `buying_tail`, `selling_tail`), `extension_above`/`extension_below`, `up_conf`/`down_conf`, and two JSONB columns: `single_prints` as contiguous ranges (`[{"from":63000,"to":63075}]`, not v1's flat bucket list) and `profile` as bucket → TPO count, which is what composites merge. `composite_id` is a nullable FK to `composites` (NULL = in no composite).

**`composites`** — `id SERIAL PK`, `symbol` FK, `start_date`/`end_date` inclusive, `days` (CHECK ≥ 2), `status` (CHECK in `open`/`closed`), merged `profile` JSONB, `poc`/`vah`/`val`/`high`/`low`, `single_prints` JSONB, nullable `poor_high`/`poor_low`, `engine_version`. Written by PR 6; empty today.

**Storage model:** raw ticks are never persisted. `trades` (7,377,603 rows) and `staging_trades` were dropped when the schema was applied and the database went **855 MB → 10 MB**. Everything the v1 SQL path had computed from those ticks is preserved as JSON in `tests/fixtures/v1_golden/` — 8 days: 2026-07-07, 07-13, 07-14, 07-15, 07-16, 07-27, 07-29, 07-30.

---

## 3. Ingestion — none in the repo (PR 2 removed the v1 paths)

There is currently no code that puts data in. The only write path is `db.upsert_daily_levels(row)`; PR 3 adds the producer that calls it.

**3a. `fetchDayRecords(date_ms)` — live REST path.** Deleted in PR 2 (in git history). It was the last user of `requests`, `psycopg2` and `execute_values` inside `ingest.py`, and it wrote to a table that no longer exists. Its millisecond-vs-microsecond divergence from the archive path dies with it.

**3b. `load_day_from_archive(date_str)` — bulk archive path.** Never in the repo (written on the inaccessible machine), so PR 2 had nothing to delete. What it proved — stream the daily zip, one pass over the CSV — is what PR 3 re-implements in memory. No COPY, no staging table, no `statement_timeout` bump, because nothing is written server-side any more.

**3c. Backfill loop.** Never in the repo. The missing `conn.rollback()` requirement moved to PR 7, where backfill is re-implemented.

**3d. Connection.** Moved out of `ingest.py` into `db.py`: `get_connection()` / `get_cursor()`, still lazy off `CONNECTION_STRING`, still no import-time side effects — importing `db` or `ingest` opens nothing, which is what lets the pure tests run with no database and no `.env`. `upsert_daily_levels` commits per call, so one session is one transaction (what PR 7's per-day rollback needs).

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

## 6. Tests

**`tests/test_engine.py`** — 5 pure unit tests, no DB and no network.
- `test_tail_is_not_double_distribution` — synthetic single-distribution + long tail → False.
- `test_genuine_double_distribution` — synthetic thick–thin–thick → True.
- `test_classifies_directional_when_one_sided_no_trend` — classifier branch check.
- `test_poor_high_derives_from_day_high_and_poor_low_from_day_low`, `test_tails_are_none_when_no_single_print_buckets` — PR 1 fixes; both patch `get_profile_grid` and `detect_trend` so no database is touched.
- `test_raises_on_missing_data` — **deleted in PR 2.** It asserted `ValueError` on an un-ingested date; with `trades` dropped every date raises, so the test could no longer fail and proved nothing.

**`tests/test_db.py`** — PR 2 acceptance. 7 tests need a live database, 2 do not.
- Schema: `schema.sql` applies to an empty namespace (a throwaway schema created inside a transaction that is rolled back, with `search_path` pointed only at it so the DROPs cannot reach `public`); its columns, nullability and PK match the constants in `db.py`; the BTCUSDT seed row is exact; `composites.days >= 2` is enforced.
- Round trip: a fixture row upserts and reads back equal, JSONB included; a second upsert on the same `(symbol, session_date)` updates rather than duplicating; `get_instrument` returns the seeded config and `None` for an unknown symbol. These use `session_date = 1970-01-01` (no archive can ever exist for it) and delete the row before and after.
- Pure: `upsert_daily_levels` rejects unknown column keys and requires the key columns.

There is no `tests/conftest.py` — §1 used to claim one.

Run: `python -m pytest -v`. **14 passed** locally with a database; **7 passed / 7 skipped** without one, which is what CI does (GitHub Actions, added in PR 1, provides no `CONNECTION_STRING`).

---

## 7. Known bugs / debts (the list the v2 spec should address or consciously defer)

**Correctness — verify before anything else**
- Backfill loop lacks `conn.rollback()` in the `except` (cascade failure). **Deferred, not fixed:** the loop and `load_day_from_archive` are not in the repo (§3b), so PR 1 had nothing to patch. The requirement moved to PR 7 (Scheduled warm-up), which is where backfill is re-implemented.

**Architecture**
- ~~Raw ticks retained forever~~ — **closed in PR 2.** The tick tables are dropped, `daily_levels`/`composites` are the only storage, and nothing in the repo can write a tick. The producer that fills `daily_levels` arrives in PR 3.
- Single module holds computation and rendering (~240 lines after PR 2 removed ingestion and connection handling). Splitting compute from rendering is still open.
- Instrument config columns now exist in the shape the engine needs (`bucket_size`, `period_seconds`, `ib_periods`, `archive_url_template`) and are seeded — but engine code still hardcodes 25, 1800 and periods 0–1, in the two `ingest.py` queries that PR 3 replaces. Reading the columns is PR 3's job.
- Session hardwired to UTC calendar day — no session-template concept.
- ~~REST path (ms) and archive path (µs) diverge~~ — **closed in PR 2** by deleting the REST path; PR 3 leaves a single archive path (µs).
- `arr_single_tpo` is a flat list of bucket prices, not contiguous ranges — a consumer can't tell one single-print zone from three.

**API**
- Millisecond-epoch path parameter instead of dates; no instrument in the route; no listing/range/cross-day; no typed schemas; no caching; recomputes on every hit.

**Ops**
- ~~Supabase project must be recreated~~ — **closed in PR 2**, but by reuse rather than recreation (owner decision): the project came back, `sql/001_drop_v1.sql` + `sql/schema.sql` replaced its contents, and `CONNECTION_STRING` therefore never changed, so Railway needs no new secret — only the redeploy it already needed. README still describes the v1 ingestion path and the `/profile/{ms}` routes; V2_SPEC assigns that rewrite to PR 4.
- No scheduled ingestion — data only exists for days manually loaded.
- No linting, no dependency pinning. (CI added in PR 1: GitHub Actions runs `python -m pytest` on push and pull request.)

---

## 8. What is genuinely done and should be preserved

- The detector logic (POC tie-break, 70% VA expansion, IB, DD split with synthetic verification, one-timeframing confidence, classifier decision tree with reasons) — validated, keep.
- The COPY → staging → INSERT…SELECT bulk-load pattern — correct for a tick store, but v2 has no tick store, so PR 2 kept only the idea (one streaming pass over the daily archive) and not the code. Recorded in git history and §3b.
- Lazy connection (now in `db.py`) + `__main__` guard — kept.
- `compute_structures` as the single source of truth feeding both JSON and text — keep the shape; it becomes the `daily_levels` row.
- Dockerfile / Railway deploy path — keep.
