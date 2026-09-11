# Market Structure Engine — Current State (Sept 2026; v1 inventory + PRs 1–2)

Baseline document. Everything the system does today, how it does it, and the debts to spec against. No proposals in here — the v2 spec is a separate doc.

---

## 1. Components

| File / thing | Role | Status |
|---|---|---|
| `ingest.py` | The engine: archive download, streaming aggregation, all detectors, classifier, report generator, and `compute_day` | ~400 lines. No SQL at all since PR 3 — `get_profile_grid` and `compute_profile` are deleted and `detect_trend` takes period ranges. Every parameter comes from `instruments` |
| `db.py` | The only module holding SQL against the v2 tables: lazy `get_connection`/`get_cursor`, `get_instrument`, `get_daily_levels`, `upsert_daily_levels`, `list_daily_levels` | Added in PR 2; storage only, computes nothing |
| `sql/schema.sql` | v2 DDL (§2) + BTCUSDT seed. Constructive only and idempotent (`CREATE TABLE IF NOT EXISTS`, seed `ON CONFLICT DO NOTHING`), so re-applying it cannot lose a computed row | Added in PR 2 and applied |
| `sql/001_drop_v1.sql` | The v1 teardown: `DROP TABLE IF EXISTS trades, staging_trades, instruments`. Kept apart from `schema.sql` so destructive and constructive statements never share a script | **Already applied (2026-09-10) — not to be re-run.** Nothing left to drop |
| `scripts/make_synthetic_day.py` | Generates `tests/fixtures/synthetic_day.csv` (312 rows) from a TPO profile written out in its docstring, so the no-network golden test's expected values are auditable rather than whatever the code happened to print | Added in PR 3; re-runnable, output committed |
| `scripts/capture_v1_golden.py` | One-shot: ran the v1 SQL path over every ingested day into `tests/fixtures/v1_golden/*.json` before the drop | Ran once; cannot be re-run (its table is gone). PR 3's regression baseline |
| `api.py` | FastAPI wrapper — 2 endpoints + auto `/docs` | **Broken, knowingly.** Both routes call `compute_structures(start_ts)`, a signature PR 3 replaced; nothing imports them and PR 4 rewrites the module. See §7 |
| `tests/test_engine.py`, `tests/test_db.py`, `tests/test_compute.py`, `pytest.ini` | 5 pure detector tests, 12 storage tests (8 needing a database), 27 compute tests (17 pure, 9 needing network + a database, 1 slow). `pytest.ini` registers the `network` and `slow` markers. There is no `conftest.py` | 44 pass locally; 26 pass / 18 skip with neither network nor database, which is what CI runs |
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

## 3. Ingestion — one streaming archive path (PR 3)

`compute_day(symbol, session_date)` is the only way data enters the system. It reads the `instruments` row, downloads that day's archive, aggregates it in one pass, runs the detectors, and upserts one `daily_levels` row. Nothing is written unless every step succeeds, so a failure never leaves a partial row.

**3a. Download.** `open_archive_csv(url)` streams the response to a `tempfile.TemporaryFile` in 1 MB chunks and opens the zip from there. Streaming to disk rather than into a `BytesIO` is deliberate: `zipfile` needs a seekable source, and a 5M-row day is a ~75 MB zip around a ~430 MB CSV. The URL comes from `archive_url_template` (`{symbol}` / `{date}`) — no URL is written in the code. HTTP 404 → `SessionNotPublished` (the archive appears ~02:00 UTC the next day); any other HTTP failure, a connection error, a corrupt zip, or a zip that does not hold exactly one member → `ArchiveUnavailable`.

**3b. Aggregation.** `aggregate_archive(csv_stream, bucket_size, period_seconds)` makes one pass with `csv.reader` over a `TextIOWrapper`, one row at a time, and returns `({bucket: {periods}}, {period: (high, low)})`. Columns are positional and headerless — `[0]=agg_trade_id, [1]=price, [2]=quantity, [5]=timestamp_micro, [6]=is_buyer_maker` — and **the timestamp is microseconds**, the divergence that used to separate the two v1 paths. A header row is tolerated (Binance has shipped archives both ways); a malformed row anywhere else raises. `bucket = floor(price / bucket_size) * bucket_size` and `period = floor(seconds_since_utc_midnight / period_seconds)`, both from config.

**Memory.** State is bounded by buckets × periods, not by row count: 400 buckets × 48 periods is under 1 MB. Measured on a 5M-row day, `tracemalloc` peak 0.9 MB, peak process RSS 43.1 MB; end to end through `compute_day` on 2026-07-30 (1.44M rows, a 21.6 MB zip), peak RSS **53.9 MB** against the 256 MB ceiling (V2_SPEC D2).

**3c. What the v1 paths left behind.** `fetchDayRecords` (REST, milliseconds) was deleted in PR 2. `load_day_from_archive` (COPY → staging → INSERT…SELECT) was never in the repo; PR 3 re-implements the idea it proved — one streaming pass over the daily zip — with no staging table, no COPY and no server-side write. The backfill loop and its missing `conn.rollback()` are still PR 7's.

**3d. Connection.** Unchanged from PR 2: lazy in `db.py`, no import-time side effects. `ingest.py` holds no SQL and no cursor — it calls `db.get_instrument` and `db.upsert_daily_levels` and nothing else.

---

## 4. Analysis pipeline (per UTC calendar day, single instrument)

Session = UTC 00:00–24:00. Bucket = $25. Period = 30 min (48 periods, lettered A–Z then a–v).

1. **`aggregate_archive(csv_stream, bucket_size, period_seconds)`** — one streaming pass over the archive CSV (§3b) → `{bucket: {periods}}` and `{period: (high, low)}`. Replaces the SQL GROUP BY over `trades`; **deleted in PR 3** along with `compute_profile`, which existed only to reshape its rows. Trade count and volume are no longer collected at all — nothing consumed them.
2. *(was `compute_profile`)* — gone; `aggregate_archive` returns the profile directly, as sets rather than lists, which the detectors read identically.
3. **`compute_poc(levels, counts)`** — bucket with max TPO count; tie-break = closest to range midpoint `(high+low)/2`.
4. **`compute_value_area(poc, levels, counts)`** — expand outward from POC one bucket at a time, taking whichever neighbour (above/below) has the larger count, until captured ≥ 70% of total TPOs. Returns `(vah, val)`.
5. **`compute_ib(profile, ib_periods=2)`** — IB = all buckets touched in the first `ib_periods` periods → `(ib_high, ib_low)`. `ib_periods` comes from `instruments`; the default reproduces v1's hardcoded "period 0 or 1" exactly. Same pass also collects `arr_single_tpo` = buckets with exactly one TPO (single prints, as a raw list of bucket prices, anywhere in the profile).
6. **`segment_profile(levels, counts, thin_max=1)`** — walk buckets top-down into runs: `thick` (count ≥ 2) / `thin` (count ≤ 1). Returns `[["thick",[...]],["thin",[...]],...]`.
7. **`detect_double_distribution_split(segments, min_thick_levels=3)`** — True if any `thick(≥3 levels) → thin → thick(≥3 levels)` sequence exists. Verified against synthetic tail-vs-DD cases (unit tests).
8. **`detect_trend(period_ranges)`** — takes the `{period: (high, low)}` map `aggregate_archive` already collected; the second SQL aggregation is gone. Arithmetic unchanged: sorting by period reproduces the old `ORDER BY period`, uptrend violated when a period's low < previous period's low, downtrend violated when a period's high > previous high, returns `(1 − up_viol/(n−1), 1 − down_viol/(n−1))`. Still raises `ValueError` under 2 periods. Prices are raw, not bucketed, as in v1.
9. **`classify_day_type(...)`** — extension multiples `ext_up = (day_high − ib_high)/ib_range`, `ext_down = (ib_low − day_low)/ib_range`. Thresholds `SMALL=0.4, MEANINGFUL=0.6, TREND_THRESH=0.8`. Decision order:
   Non-Trend (both < 0.4) → Neutral (both > 0.6) → Double-Distribution Trend (is_dd) → Trend down (down_conf > 0.8) → Trend up (up_conf > 0.8) → Directional (unclassified) (either side > 0.6) → Normal. Returns `(label, reason_string)`.
10. **`compute_structures(profile, period_ranges, symbol, session_date, bucket_size, ib_periods)`** — orchestrates 3–9 over an in-memory profile. Pure: no I/O, no clock, no database. Raises `NoTradeData` (a `ValueError`) on an empty profile. Returns a dict keyed **exactly on `daily_levels` columns** so it goes straight to `db.upsert_daily_levels`: `symbol`, `session_date`, `engine_version`, `day_type`, `reason`, `poc`, `vah`, `val`, `ib_high`, `ib_low`, `day_high`, `day_low`, `poor_high`, `poor_low`, `buying_tail`, `selling_tail`, `extension_above`, `extension_below`, `up_conf`, `down_conf`, `single_prints`, `profile`. Two shape changes from v1: `date` → `session_date` (a `date`, not a string), and `arr_single_tpo` (flat bucket list) → `single_prints` (contiguous ranges, `[{"from": x, "to": y}]`). `computed_at` is left to the schema default and `composite_id` to PR 6.
11. **`compute_day(symbol, session_date)`** — config → download → aggregate → 10 → upsert. The only entry point, and the only place the three of them meet.
12. **`generate_report(structures)`** — unchanged text block, reading `symbol` and `session_date` off the row instead of a hardcoded "BTC/USDT" and a `date` string.

**Validation history:** POC/VA/IB matched Exocharts within 1–2 buckets on July 7 2026; DD detector verified on synthetic cases; classifier output on July 7 = "Directional (unclassified), extended 4.1x IB down; one-timeframing 0.60".

**PR 3 parity:** the in-memory path reproduces the v1 Postgres path on **all 8 golden days** (`tests/fixtures/v1_golden/`) — exact equality on `day_type`, on the `reason` string, on every bucket-valued field (POC, VAH, VAL, IB, day high/low, tails, poor extremes) and on the set of single-print buckets, with the two extension ratios equal to within 1e-12 (v1 divided in `Decimal`, v2 in `float`). The spec asked only for 2026-07-27 and only within one bucket.

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
- `test_poor_high_derives_from_day_high_and_poor_low_from_day_low`, `test_tails_are_none_when_no_single_print_buckets` — PR 1 fixes. They assert what they always did; PR 3 dropped the monkeypatching of `get_profile_grid`/`detect_trend` they used to need, because `compute_structures` now takes the synthetic profile as an argument.
- `test_raises_on_missing_data` — **deleted in PR 2.** It asserted `ValueError` on an un-ingested date; with `trades` dropped every date raises, so the test could no longer fail and proved nothing.

**`tests/test_compute.py`** — PR 3 acceptance. 27 tests: 17 pure, 9 needing network + a database, 1 slow.
- Parity: `compute_day("BTCUSDT", d)` against each of the 8 v1 golden days (see §4). Needs the archive and the instrument config, so it is marked `network` and skipped when `CI` is set; a local `python -m pytest` runs it.
- Golden, no network: `tests/fixtures/synthetic_day.csv` (312 rows, generated by `scripts/make_synthetic_day.py` from a profile written out in its docstring) → POC 63100, VA 63000–63175, IB 63000–63150.
- Units: bucket and period from config, microsecond timestamps, tolerated header row, `detect_trend` violation counting and its < 2 period guard, single prints as contiguous ranges, unknown symbol, `SessionNotPublished` and `ArchiveUnavailable` with no row written (HTTP and storage both stubbed, so these run in CI).
- Memory: 5M rows through `aggregate_archive` under the 256 MB ceiling (`slow`, skipped in CI), plus a fast test that the accumulator does not grow with row count.

**`tests/test_db.py`** — PR 2 acceptance. 8 tests need a live database, 4 do not.
- Schema: `schema.sql` applies to an empty namespace (a throwaway schema created inside a transaction that is rolled back, with `search_path` pointed only at it so the DROPs cannot reach `public`); its columns, nullability and PK match the constants in `db.py`; the BTCUSDT seed row is exact; `composites.days >= 2` is enforced.
- Round trip: a fixture row upserts and reads back equal, JSONB included; a second upsert on the same `(symbol, session_date)` updates rather than duplicating; `get_instrument` returns the seeded config and `None` for an unknown symbol. These use `session_date = 1970-01-01` (no archive can ever exist for it) and delete the row before and after.
- Pure: `upsert_daily_levels` rejects unknown column keys and requires the key columns.

There is no `tests/conftest.py` — §1 used to claim one.

Run: `python -m pytest -v`. **44 passed in 2m 46s** locally (the 8 parity days download ~110 MB of archives; the 5M-row memory test is pure compute). **26 passed / 18 skipped** with neither network nor database, which is what CI does — GitHub Actions provides no `CONNECTION_STRING` and sets `CI=true`, which is what the `network` and `slow` tests skip on. Locally, `-m "not network"` skips the downloads by hand and `-m network` runs only the parity days.

---

## 7. Known bugs / debts (the list the v2 spec should address or consciously defer)

**Correctness — verify before anything else**
- Backfill loop lacks `conn.rollback()` in the `except` (cascade failure). **Deferred, not fixed:** the loop and `load_day_from_archive` are not in the repo (§3b), so PR 1 had nothing to patch. The requirement moved to PR 7 (Scheduled warm-up), which is where backfill is re-implemented.

**Architecture**
- ~~Raw ticks retained forever~~ — **closed in PR 2.** The tick tables are dropped, `daily_levels`/`composites` are the only storage, and nothing in the repo can write a tick. The producer that fills `daily_levels` arrives in PR 3.
- Single module holds download, computation and rendering (~400 lines after PR 3 added the archive path). Splitting them is still open, and is now the largest structural debt in the repo.
- ~~Instrument config columns exist but are not read~~ — **closed in PR 3.** `compute_day` reads `bucket_size`, `period_seconds`, `ib_periods` and `archive_url_template` off the `instruments` row and threads them through `aggregate_archive`, `compute_ib` and the URL. No symbol, bucket, period or URL is written down in engine code; the tests assert this by aggregating the same rows at three different bucket sizes and two period widths.
- Session hardwired to UTC calendar day — no session-template concept.
- ~~REST path (ms) and archive path (µs) diverge~~ — **closed in PR 2** by deleting the REST path; PR 3 leaves a single archive path (µs).
- ~~`arr_single_tpo` is a flat list of bucket prices~~ — **closed in PR 3.** `single_prints` is now `[{"from": x, "to": y}]`, contiguous in whole buckets, so one zone is distinguishable from three. The parity test expands the ranges back to buckets to compare against v1.

**API**
- **`api.py` is broken as of PR 3** and left that way deliberately: both routes call `compute_structures(start_ts)`, whose signature changed to take an in-memory profile. Nothing imports `api.py` and PR 4 replaces both routes with the date-based, typed ones, so fixing it here would be work thrown away a PR later.
- Millisecond-epoch path parameter instead of dates; no instrument in the route; no listing/range/cross-day; no typed schemas; no caching; recomputes on every hit.

**Ops**
- ~~Supabase project must be recreated~~ — **closed in PR 2**, but by reuse rather than recreation (owner decision): the project came back, `sql/001_drop_v1.sql` + `sql/schema.sql` replaced its contents, and `CONNECTION_STRING` therefore never changed, so Railway needs no new secret — only the redeploy it already needed. README still describes the v1 ingestion path and the `/profile/{ms}` routes; V2_SPEC assigns that rewrite to PR 4.
- No scheduled ingestion — data only exists for days computed on demand. The 8 golden days are cached in `daily_levels` as a side effect of the parity test; everything else is a cache miss until PR 7's warm-up.
- No linting, no dependency pinning. (CI added in PR 1: GitHub Actions runs `python -m pytest` on push and pull request.)

---

## 8. What is genuinely done and should be preserved

- The detector logic (POC tie-break, 70% VA expansion, IB, DD split with synthetic verification, one-timeframing confidence, classifier decision tree with reasons) — validated, keep.
- The COPY → staging → INSERT…SELECT bulk-load pattern — correct for a tick store, but v2 has no tick store, so PR 2 kept only the idea (one streaming pass over the daily archive) and not the code. Recorded in git history and §3b.
- Lazy connection (now in `db.py`) + `__main__` guard — kept.
- `compute_structures` as the single source of truth feeding both JSON and text — kept, and since PR 3 its return value **is** the `daily_levels` row, keyed on the table's own columns.
- Dockerfile / Railway deploy path — keep.
