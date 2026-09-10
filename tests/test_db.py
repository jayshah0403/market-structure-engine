"""Acceptance tests for V2_SPEC PR 2 (new data layer).

Both DB-backed groups need a real Postgres because what is under test *is* the
DDL and the driver's JSONB round-trip; they skip without CONNECTION_STRING, so
CI stays green without a database (V2_SPEC section 4, "Tests"). The validation
tests at the bottom need neither network nor DB and always run.
"""

import datetime
import os
import uuid

import pytest

import db

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCHEMA_SQL_PATH = os.path.join(REPO_ROOT, "sql", "schema.sql")
# Already applied; here only so the separation between the two files can be
# asserted. No test ever executes it.
DROP_SQL_PATH = os.path.join(REPO_ROOT, "sql", "001_drop_v1.sql")

needs_db = pytest.mark.skipif(
    "CONNECTION_STRING" not in os.environ,
    reason="integration test: needs a live database",
)

# Nullable exactly where V2_SPEC 2.2 annotates "NUMERIC NULL" / "INT NULL".
NULLABLE_DAILY_LEVELS_COLUMNS = {
    "poor_high", "poor_low", "buying_tail", "selling_tail", "composite_id",
}


@pytest.fixture
def empty_schema():
    """A throwaway, empty Postgres schema, rolled back afterwards.

    schema.sql is applied inside a transaction against a fresh schema whose name
    is the only entry on the search_path, so its CREATEs and the FKs between them
    resolve inside that namespace and cannot touch `public`. Postgres makes DDL
    transactional, so the rollback removes the schema and everything created in
    it; the test leaves no trace.
    """
    import psycopg2

    conn = psycopg2.connect(os.environ["CONNECTION_STRING"])
    schema = "pr2_schema_check_" + uuid.uuid4().hex[:8]
    cur = conn.cursor()
    try:
        cur.execute('CREATE SCHEMA "' + schema + '"')
        cur.execute('SET LOCAL search_path TO "' + schema + '"')
        yield cur, schema
    finally:
        conn.rollback()
        conn.close()


def _apply_schema(cur):
    with open(SCHEMA_SQL_PATH, encoding="utf-8") as handle:
        cur.execute(handle.read())


def _columns(cur, schema, table):
    cur.execute(
        """SELECT column_name, is_nullable
             FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position""",
        (schema, table),
    )
    return cur.fetchall()


@needs_db
def test_schema_applies_cleanly_to_an_empty_database(empty_schema):
    cur, schema = empty_schema

    _apply_schema(cur)

    cur.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = %s ORDER BY table_name",
        (schema,),
    )
    assert [row[0] for row in cur.fetchall()] == [
        "composites", "daily_levels", "instruments",
    ]


@needs_db
def test_schema_is_idempotent(empty_schema):
    """Re-applying schema.sql must be a no-op, not an error or a data loss.

    This is what makes it safe to run against the live database once
    daily_levels holds rows that cost 10-30 s each to recompute.
    """
    cur, schema = empty_schema
    _apply_schema(cur)
    cur.execute("INSERT INTO daily_levels (symbol, session_date, engine_version,"
                " day_type, reason, poc, vah, val, ib_high, ib_low, day_high,"
                " day_low, extension_above, extension_below, up_conf, down_conf,"
                " single_prints, profile)"
                " VALUES ('BTCUSDT', '2026-07-27', 1, 'Neutral', 'fixture',"
                " 1, 1, 1, 1, 1, 1, 1, 0, 0, 0.5, 0.5, '[]', '{}')")

    _apply_schema(cur)  # second application, same transaction

    cur.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = %s ORDER BY table_name",
        (schema,),
    )
    assert [row[0] for row in cur.fetchall()] == [
        "composites", "daily_levels", "instruments",
    ]
    # The computed row survived, and the seed did not double up.
    cur.execute("SELECT count(*) FROM daily_levels")
    assert cur.fetchone()[0] == 1
    cur.execute("SELECT count(*) FROM instruments")
    assert cur.fetchone()[0] == 1


@needs_db
def test_schema_columns_match_the_storage_layer(empty_schema):
    """schema.sql and db.py must not drift apart."""
    cur, schema = empty_schema
    _apply_schema(cur)

    daily = _columns(cur, schema, "daily_levels")
    assert [name for name, _ in daily] == list(db.DAILY_LEVELS_COLUMNS)
    nullable = {name for name, is_nullable in daily if is_nullable == "YES"}
    assert nullable == NULLABLE_DAILY_LEVELS_COLUMNS

    instruments = _columns(cur, schema, "instruments")
    assert [name for name, _ in instruments] == list(db.INSTRUMENT_COLUMNS)

    # daily_levels is keyed on (symbol, session_date) - what upsert relies on.
    cur.execute(
        """SELECT kcu.column_name
             FROM information_schema.table_constraints tc
             JOIN information_schema.key_column_usage kcu
               ON kcu.constraint_name = tc.constraint_name
              AND kcu.table_schema = tc.table_schema
            WHERE tc.table_schema = %s AND tc.table_name = 'daily_levels'
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position""",
        (schema,),
    )
    assert [row[0] for row in cur.fetchall()] == list(db.DAILY_LEVELS_KEY)


@needs_db
def test_schema_seeds_btcusdt_with_engine_config(empty_schema):
    cur, schema = empty_schema
    _apply_schema(cur)

    cur.execute("SELECT symbol, exchange, bucket_size, period_seconds, "
                "ib_periods, archive_url_template, active FROM instruments")
    assert cur.fetchall() == [(
        "BTCUSDT", "binance-spot", 25, 1800, 2,
        "https://data.binance.vision/data/spot/daily/aggTrades/"
        "{symbol}/{symbol}-aggTrades-{date}.zip",
        True,
    )]


@needs_db
def test_composites_rejects_a_single_day_composite(empty_schema):
    """V2_SPEC PR 6: a composite is emitted only if it holds >= 2 days."""
    import psycopg2.errors

    cur, _schema = empty_schema
    _apply_schema(cur)

    cur.execute("SAVEPOINT before_bad_insert")
    with pytest.raises(psycopg2.errors.CheckViolation):
        cur.execute(
            "INSERT INTO composites (symbol, start_date, end_date, days, status,"
            " profile, poc, vah, val, high, low, single_prints, engine_version)"
            " VALUES ('BTCUSDT', '2026-07-13', '2026-07-13', 1, 'open',"
            " '{}', 1, 1, 1, 1, 1, '[]', 1)"
        )
    cur.execute("ROLLBACK TO SAVEPOINT before_bad_insert")


# --- round trip -------------------------------------------------------------

# An unmistakably synthetic session_date: no Binance archive exists for 1970, so
# this row can never collide with a computed one. The symbol is BTCUSDT because
# daily_levels.symbol is a FK onto the seeded instruments row.
FIXTURE_ROW = {
    "symbol": "BTCUSDT",
    "session_date": datetime.date(1970, 1, 1),
    "engine_version": 1,
    "day_type": "Neutral",
    "reason": "Extended both sides (0.9x up, 4.1x down) - two-sided, responsive",
    "poc": 65200.0,
    "vah": 65425.0,
    "val": 64750.0,
    "ib_high": 65400.0,
    "ib_low": 65050.0,
    "day_high": 65725.0,
    "day_low": 63600.0,
    "poor_high": None,
    "poor_low": 63600.0,
    "buying_tail": None,
    "selling_tail": 65725.0,
    "extension_above": 0.9285714285714286,
    "extension_below": 4.142857142857143,
    "up_conf": 0.6,
    "down_conf": 0.55,
    # JSONB: contiguous ranges, not a flat bucket list (V2_SPEC 2.2).
    "single_prints": [{"from": 63600, "to": 63675}, {"from": 65700, "to": 65725}],
    # JSONB: bucket -> TPO count.
    "profile": {"65200": 19, "65225": 18, "64750": 3},
    "composite_id": None,
}


@pytest.fixture
def clean_fixture_row():
    """Delete the fixture row before and after, so the test is repeatable.

    upsert_daily_levels commits (one session is one transaction), so this test
    cannot be wrapped in a rollback the way the schema tests are.
    """
    def delete():
        conn = db.get_connection()
        cur = db.get_cursor()
        cur.execute(
            "DELETE FROM daily_levels WHERE symbol = %s AND session_date = %s",
            (FIXTURE_ROW["symbol"], FIXTURE_ROW["session_date"]),
        )
        conn.commit()

    delete()
    try:
        yield
    finally:
        delete()


@needs_db
def test_fixture_row_round_trips_through_upsert_and_get(clean_fixture_row):
    db.upsert_daily_levels(FIXTURE_ROW)

    stored = db.get_daily_levels(FIXTURE_ROW["symbol"], FIXTURE_ROW["session_date"])

    assert stored is not None
    # computed_at is server-side (DEFAULT now()) and not part of the fixture.
    assert stored.pop("computed_at").tzinfo is not None
    assert stored == FIXTURE_ROW
    # JSONB comes back as parsed Python, not as text.
    assert stored["single_prints"][0] == {"from": 63600, "to": 63675}
    assert stored["profile"]["65200"] == 19


@needs_db
def test_upsert_replaces_the_row_for_the_same_symbol_and_date(clean_fixture_row):
    db.upsert_daily_levels(FIXTURE_ROW)
    updated = dict(FIXTURE_ROW, day_type="Trend (up)", poc=65225.0,
                   profile={"65225": 25})

    db.upsert_daily_levels(updated)

    rows = db.list_daily_levels(FIXTURE_ROW["symbol"],
                                datetime.date(1970, 1, 1),
                                datetime.date(1970, 1, 2))
    assert len(rows) == 1
    assert rows[0]["day_type"] == "Trend (up)"
    assert rows[0]["poc"] == 65225.0
    assert rows[0]["profile"] == {"65225": 25}


@needs_db
def test_get_instrument_reads_the_seeded_engine_config():
    instrument = db.get_instrument("BTCUSDT")

    assert instrument["bucket_size"] == 25.0
    assert instrument["period_seconds"] == 1800
    assert instrument["ib_periods"] == 2
    assert "{symbol}" in instrument["archive_url_template"]
    assert db.get_instrument("NOPE") is None


# --- the two SQL files stay separated (no database) -------------------------

def _statements(path):
    """The file's SQL with comment lines stripped, lowercased."""
    with open(path, encoding="utf-8") as handle:
        lines = [line for line in handle
                 if not line.lstrip().startswith("--")]
    return " ".join(lines).lower()


def test_schema_sql_contains_no_destructive_statement():
    """Destructive and constructive statements must not share a script.

    schema.sql is re-runnable, so nothing in it may destroy anything: the v1
    teardown lives in sql/001_drop_v1.sql. Comment lines are stripped first -
    the header talks about dropping without doing any.
    """
    sql = _statements(SCHEMA_SQL_PATH)

    for statement in ("drop ", "truncate", "delete from", "alter table"):
        assert statement not in sql, statement
    assert sql.count("create table if not exists") == 3
    assert "on conflict (symbol) do nothing" in sql


def test_drop_script_contains_no_constructive_statement():
    sql = _statements(DROP_SQL_PATH)

    assert "drop table if exists trades, staging_trades, instruments;" in sql
    for statement in ("create ", "insert "):
        assert statement not in sql, statement


# --- validation (no database) ----------------------------------------------

def test_upsert_rejects_unknown_columns():
    with pytest.raises(ValueError, match="unknown daily_levels columns"):
        db.upsert_daily_levels(dict(FIXTURE_ROW, arr_single_tpo=[1, 2]))


def test_upsert_requires_the_key_columns():
    without_date = {key: value for key, value in FIXTURE_ROW.items()
                    if key != "session_date"}
    with pytest.raises(ValueError, match="key columns are required"):
        db.upsert_daily_levels(without_date)
