"""Storage layer. The only module that knows about Postgres.

V2_SPEC PR 2: storage only — nothing here downloads or computes anything.
The connection is lazy (V2_SPEC PR 2 interface; CURRENT_STATE 3d), so importing
this module has no side effects and the pure engine tests run with no database
and no `.env`.
"""

import os

import psycopg2
# pyrefly: ignore [missing-import]
from dotenv import load_dotenv

load_dotenv()

_conn = None
_cur = None


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
