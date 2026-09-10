"""Capture the v1 (Postgres-backed) compute_structures output for every UTC day
present in `trades`, as PR 3's regression baseline.

Run this BEFORE `sql/001_drop_v1.sql` drops `trades`: it is the only way to preserve
what the v1 SQL path produced. Once the table is gone the script cannot be
re-run — it is committed as the provenance record for
`tests/fixtures/v1_golden/*.json`, not as a reusable tool.

    python scripts/capture_v1_golden.py

Writes one file per day, `tests/fixtures/v1_golden/<YYYY-MM-DD>.json`, holding
the `compute_structures` dict verbatim. Decimals (price buckets arrive from
psycopg2 as `Decimal`) are written as JSON numbers; every other value is already
a float, str or list.
"""

import json
import os
import sys
from datetime import timezone
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_cursor  # noqa: E402
import ingest  # noqa: E402

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tests", "fixtures", "v1_golden",
)


def discover_days():
    """Every distinct UTC calendar day with at least one trade, ascending.

    The session timezone is pinned to UTC so `date_trunc('day', ts)` truncates
    on the same boundary the engine uses (V2_SPEC 2.2: session_date is a UTC
    calendar day).
    """
    cur = get_cursor()
    cur.execute("SET TIME ZONE 'UTC'")
    cur.execute(
        "SELECT DISTINCT date_trunc('day', ts) AS day FROM trades ORDER BY day"
    )
    return [row[0].replace(tzinfo=timezone.utc) for row in cur.fetchall()]


def _json_default(value):
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"cannot serialise {type(value).__name__}: {value!r}")


def main():
    os.makedirs(FIXTURE_DIR, exist_ok=True)
    days = discover_days()
    print(f"{len(days)} day(s) present in trades")

    captured, failed = [], []
    for day in days:
        date_str = day.strftime("%Y-%m-%d")
        start_ts_ms = int(day.timestamp() * 1000)
        try:
            structures = ingest.compute_structures(start_ts_ms)
        except ValueError as exc:
            # A day can exist in `trades` yet be uncomputable: detect_trend
            # needs >= 2 periods. Record it and keep going.
            failed.append((date_str, str(exc)))
            print(f"  SKIP {date_str}: {exc}")
            continue
        path = os.path.join(FIXTURE_DIR, f"{date_str}.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(structures, handle, indent=2, sort_keys=True,
                      default=_json_default)
            handle.write("\n")
        captured.append(date_str)
        print(f"  OK   {date_str} -> {os.path.relpath(path)} "
              f"({structures['day_type']})")

    print(f"\ncaptured {len(captured)}: {', '.join(captured)}")
    if failed:
        print(f"failed {len(failed)}: " +
              ", ".join(f"{d} ({e})" for d, e in failed))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
