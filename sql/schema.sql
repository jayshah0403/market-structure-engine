-- Market Structure Engine — v2 schema (V2_SPEC section 2).
--
-- Persistent storage is three small tables: per-instrument config, one computed
-- row per session, and the composites those sessions belong to. Raw ticks are
-- never persisted (V2_SPEC section 0, "Guiding principle").
--
-- Apply with:  psql "$CONNECTION_STRING" -f sql/schema.sql
--
-- CONSTRUCTIVE ONLY, AND IDEMPOTENT. Every statement is CREATE TABLE IF NOT
-- EXISTS or an INSERT that does nothing on conflict, so re-running this script
-- against a live database creates what is missing and touches nothing that
-- exists — no computed daily_levels or composites row can be lost to a re-run.
-- Nothing here drops anything: the v1 teardown is sql/001_drop_v1.sql, already
-- applied and kept apart precisely so that a re-run of this file cannot reach it.
--
-- What idempotent does NOT mean here: this is not a migration tool. IF NOT
-- EXISTS skips a table that already exists whatever shape it is in, so it will
-- not add a column to, or alter, an existing table. Changing the shape of a
-- live table needs its own numbered script (002_..., ALTER TABLE), and
-- tests/test_db.py::test_schema_columns_match_the_storage_layer is what catches
-- a schema that has drifted from db.py.


-- 2.1 instruments — every per-instrument parameter the engine reads at runtime.
-- No bucket size, period, or URL is hardcoded in engine code (V2_SPEC section 4,
-- "Config").
CREATE TABLE IF NOT EXISTS instruments (
    symbol               TEXT PRIMARY KEY,
    exchange             TEXT    NOT NULL,
    bucket_size          NUMERIC NOT NULL,
    period_seconds       INT     NOT NULL,
    ib_periods           INT     NOT NULL,
    archive_url_template TEXT    NOT NULL,
    active               BOOL    NOT NULL DEFAULT TRUE
);


-- 2.3 composites — one row per emitted composite. Created before daily_levels
-- because daily_levels.composite_id references it.
CREATE TABLE IF NOT EXISTS composites (
    id             SERIAL PRIMARY KEY,
    symbol         TEXT    NOT NULL REFERENCES instruments (symbol),
    start_date     DATE    NOT NULL,
    end_date       DATE    NOT NULL,
    -- A composite is emitted only if it contains >= 2 days (V2_SPEC PR 6).
    days           INT     NOT NULL CHECK (days >= 2),
    status         TEXT    NOT NULL CHECK (status IN ('open', 'closed')),
    profile        JSONB   NOT NULL,
    poc            NUMERIC NOT NULL,
    vah            NUMERIC NOT NULL,
    val            NUMERIC NOT NULL,
    high           NUMERIC NOT NULL,
    low            NUMERIC NOT NULL,
    single_prints  JSONB   NOT NULL,
    poor_high      NUMERIC,
    poor_low       NUMERIC,
    engine_version INT     NOT NULL
);


-- 2.2 daily_levels — one row per (symbol, session_date). This row is the cached
-- primitive everything else derives from; `profile` is required for composites.
CREATE TABLE IF NOT EXISTS daily_levels (
    symbol           TEXT        NOT NULL REFERENCES instruments (symbol),
    session_date     DATE        NOT NULL,          -- UTC calendar day
    engine_version   INT         NOT NULL,          -- stale rows recompute lazily
    computed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    day_type         TEXT        NOT NULL,
    reason           TEXT        NOT NULL,
    poc              NUMERIC     NOT NULL,
    vah              NUMERIC     NOT NULL,
    val              NUMERIC     NOT NULL,
    ib_high          NUMERIC     NOT NULL,
    ib_low           NUMERIC     NOT NULL,
    day_high         NUMERIC     NOT NULL,
    day_low          NUMERIC     NOT NULL,
    -- poor_high derives from day_high, poor_low from day_low (fixed in PR 1).
    poor_high        NUMERIC,
    poor_low         NUMERIC,
    buying_tail      NUMERIC,
    selling_tail     NUMERIC,
    extension_above  NUMERIC     NOT NULL,          -- multiples of IB range
    extension_below  NUMERIC     NOT NULL,
    up_conf          NUMERIC     NOT NULL,          -- one-timeframing confidence
    down_conf        NUMERIC     NOT NULL,
    -- Contiguous ranges [{"from": 63000, "to": 63075}, ...], not a flat list.
    single_prints    JSONB       NOT NULL,
    -- {"63225": 19, "63250": 18, ...} bucket -> TPO count.
    profile          JSONB       NOT NULL,
    -- NULL = not part of any >= 2-day composite.
    composite_id     INT         REFERENCES composites (id),
    PRIMARY KEY (symbol, session_date)
);


-- Seed: BTCUSDT only (V2_SPEC section 2.1 / decision D1 — no second instrument
-- at launch). DO NOTHING rather than DO UPDATE: a re-run must not silently
-- revert a bucket_size or active flag that was changed deliberately on the live
-- instrument. Changing seeded config is an explicit UPDATE, not a re-apply.
INSERT INTO instruments (symbol, exchange, bucket_size, period_seconds,
                         ib_periods, archive_url_template, active)
VALUES ('BTCUSDT', 'binance-spot', 25, 1800, 2,
        'https://data.binance.vision/data/spot/daily/aggTrades/{symbol}/{symbol}-aggTrades-{date}.zip',
        TRUE)
ON CONFLICT (symbol) DO NOTHING;
