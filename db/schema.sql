-- Market Structure Engine — v2 schema (V2_SPEC section 2).
--
-- Persistent storage is three small tables: per-instrument config, one computed
-- row per session, and the composites those sessions belong to. Raw ticks are
-- never persisted (V2_SPEC section 0, "Guiding principle") — which is why the
-- v1 tick tables are dropped first.
--
-- Apply with:  psql "$CONNECTION_STRING" -f db/schema.sql
--
-- NOTE: this script is written for a first application against the v1 database.
-- Re-applying it over an already-migrated database fails (instruments cannot be
-- dropped while daily_levels/composites reference it, and the CREATEs are not
-- IF NOT EXISTS) — deliberately, so it cannot silently discard computed rows.

-- v1 tick storage. Retaining raw ticks forever is what filled the free tier
-- (CURRENT_STATE section 2). trades is listed before instruments because it
-- holds the FK; a single DROP handles the dependency order between them.
DROP TABLE IF EXISTS trades, staging_trades, instruments;


-- 2.1 instruments — every per-instrument parameter the engine reads at runtime.
-- No bucket size, period, or URL is hardcoded in engine code (V2_SPEC section 4,
-- "Config").
CREATE TABLE instruments (
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
CREATE TABLE composites (
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
CREATE TABLE daily_levels (
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
-- at launch).
INSERT INTO instruments (symbol, exchange, bucket_size, period_seconds,
                         ib_periods, archive_url_template, active)
VALUES ('BTCUSDT', 'binance-spot', 25, 1800, 2,
        'https://data.binance.vision/data/spot/daily/aggTrades/{symbol}/{symbol}-aggTrades-{date}.zip',
        TRUE);
