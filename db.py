"""Storage layer. The only module that knows about Postgres.

V2_SPEC PR 2: storage only — nothing here downloads or computes anything, and
nothing here imports FastAPI (V2_SPEC section 4, "Errors").

The connection is lazy (CURRENT_STATE 3d), so importing this module has no side
effects and the pure engine tests run with no database and no `.env`.

Read conventions, so callers never have to think about driver types:
  * NUMERIC columns are returned as `float` (psycopg2 hands them over as
    `Decimal`). Bucket prices are multiples of the instrument's bucket size and
    the extension/confidence values are ratios, so float is lossless here and is
    what the detectors and the JSON responses want.
  * JSONB columns are already parsed by psycopg2 into lists/dicts.
  * DATE and TIMESTAMPTZ columns stay as `datetime.date` / `datetime.datetime`.
"""

import os
from decimal import Decimal

import psycopg2
# pyrefly: ignore [missing-import]
from dotenv import load_dotenv
from psycopg2.extras import Json

load_dotenv()

_conn = None
_cur = None

# Column order of `daily_levels` in sql/schema.sql. Doubles as the whitelist that
# upsert_daily_levels validates row keys against, so a caller's dict keys are
# never interpolated into SQL unchecked.
DAILY_LEVELS_COLUMNS = (
    "symbol", "session_date", "engine_version", "computed_at",
    "day_type", "reason",
    "poc", "vah", "val",
    "ib_high", "ib_low",
    "day_high", "day_low",
    "poor_high", "poor_low",
    "buying_tail", "selling_tail",
    "extension_above", "extension_below",
    "up_conf", "down_conf",
    "single_prints", "profile",
    "composite_id",
)

# (symbol, session_date) is the primary key, so an upsert never overwrites it.
DAILY_LEVELS_KEY = ("symbol", "session_date")

# Passed to Postgres wrapped in psycopg2's Json adapter, not as Python literals.
JSONB_COLUMNS = frozenset({"single_prints", "profile"})

INSTRUMENT_COLUMNS = (
    "symbol", "exchange", "bucket_size", "period_seconds",
    "ib_periods", "archive_url_template", "active",
)


def get_connection():
    """Open (once) and return the shared connection, from CONNECTION_STRING."""
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg2.connect(os.environ["CONNECTION_STRING"])
    return _conn


def get_cursor():
    """Cursor on the shared connection. Moved here from ingest.py."""
    global _cur
    if _cur is None or _cur.closed:
        _cur = get_connection().cursor()
    return _cur


def _scalar(value):
    """Decimal -> float; everything else through untouched."""
    return float(value) if isinstance(value, Decimal) else value


def _row_to_dict(cursor, row):
    if row is None:
        return None
    return {col.name: _scalar(value) for col, value in zip(cursor.description, row)}


def get_instrument(symbol):
    """The instruments row for `symbol` as a dict, or None if unknown.

    Inactive instruments are returned too: whether `active` is a 404 is an HTTP
    decision and belongs to the API layer (V2_SPEC PR 4).
    """
    cur = get_cursor()
    cur.execute(
        f"SELECT {', '.join(INSTRUMENT_COLUMNS)} FROM instruments WHERE symbol = %s",
        (symbol,),
    )
    return _row_to_dict(cur, cur.fetchone())


def get_daily_levels(symbol, session_date):
    """The cached row for one session as a dict, or None if not cached.

    `engine_version` is returned as stored; deciding that a row is stale and
    must be recomputed is the caller's job (V2_SPEC PR 4).
    """
    cur = get_cursor()
    cur.execute(
        f"SELECT {', '.join(DAILY_LEVELS_COLUMNS)} FROM daily_levels "
        "WHERE symbol = %s AND session_date = %s",
        (symbol, session_date),
    )
    return _row_to_dict(cur, cur.fetchone())


def upsert_daily_levels(row):
    """Insert or replace one session row, keyed on (symbol, session_date).

    `row` is a dict of `daily_levels` column names. Unknown keys raise; columns
    left out fall back to their schema default (`computed_at` = now()) on insert
    and are left untouched on update, so a caller can refresh part of a row.
    Commits on success — one session is one transaction, which is what lets PR 7's
    backfill roll back a single failed day without poisoning the next.
    """
    unknown = set(row) - set(DAILY_LEVELS_COLUMNS)
    if unknown:
        raise ValueError(f"unknown daily_levels columns: {sorted(unknown)}")
    missing = [key for key in DAILY_LEVELS_KEY if row.get(key) is None]
    if missing:
        raise ValueError(f"daily_levels key columns are required: {missing}")

    columns = [col for col in DAILY_LEVELS_COLUMNS if col in row]
    values = [Json(row[col]) if col in JSONB_COLUMNS else row[col] for col in columns]
    updates = [col for col in columns if col not in DAILY_LEVELS_KEY]

    conn = get_connection()
    cur = get_cursor()
    cur.execute(
        f"INSERT INTO daily_levels ({', '.join(columns)}) "
        f"VALUES ({', '.join(['%s'] * len(columns))}) "
        f"ON CONFLICT ({', '.join(DAILY_LEVELS_KEY)}) DO UPDATE SET "
        + ", ".join(f"{col} = EXCLUDED.{col}" for col in updates),
        values,
    )
    conn.commit()


def list_daily_levels(symbol, from_date, to_date):
    """Cached rows for `symbol` in [from_date, to_date] inclusive, date ascending.

    Missing days are simply absent — reporting gaps is the API layer's job
    (V2_SPEC PR 4). `from`/`to` in the spec's signature are Python keywords,
    hence `from_date`/`to_date`.
    """
    cur = get_cursor()
    cur.execute(
        f"SELECT {', '.join(DAILY_LEVELS_COLUMNS)} FROM daily_levels "
        "WHERE symbol = %s AND session_date BETWEEN %s AND %s "
        "ORDER BY session_date",
        (symbol, from_date, to_date),
    )
    return [_row_to_dict(cur, row) for row in cur.fetchall()]
