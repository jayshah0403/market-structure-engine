"""Acceptance tests for V2_SPEC PR 3 (in-memory compute path).

Three groups, by what they need:

* **Pure** (no network, no DB, always run, including CI): the synthetic-fixture
  golden test, `aggregate_archive` unit tests, `detect_trend(period_ranges)`,
  the single-print range builder, the `SessionNotPublished` test (HTTP and
  storage both stubbed), and the memory ceiling.
* **Network + DB** (`needs_archive`): the 8-day parity run against
  `tests/fixtures/v1_golden/`. `compute_day` downloads from
  data.binance.vision and reads its config from `instruments`, so these need
  both. They are skipped when `CI` is set — GitHub Actions sets `CI=true`
  automatically, so CI skips them with no workflow change, and a local
  `python -m pytest` runs them. `-m "not network"` skips them by hand.
* **Slow** (`slow`): the 5M-row memory ceiling, skipped in CI for runtime only.
"""

import csv
import datetime
import io
import json
import math
import os
import tracemalloc

import pytest

import db
import ingest

FIXTURE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
V1_GOLDEN_DIR = os.path.join(FIXTURE_DIR, "v1_golden")
SYNTHETIC_CSV = os.path.join(FIXTURE_DIR, "synthetic_day.csv")
# Same 312 rows plus one timestamped just past midnight of the following day.
SYNTHETIC_STRAY_CSV = os.path.join(FIXTURE_DIR, "synthetic_day_with_stray_row.csv")
# Both fixtures' timestamps are offsets from epoch midnight, so the session they
# describe is 1970-01-01 (scripts/make_synthetic_day.py).
SYNTHETIC_SESSION = datetime.date(1970, 1, 1)

IN_CI = os.environ.get("CI", "").strip().lower() in ("1", "true", "yes")

needs_archive = [
    pytest.mark.network,
    pytest.mark.skipif(
        IN_CI,
        reason="downloads from data.binance.vision; skipped in CI, runs locally",
    ),
    pytest.mark.skipif(
        "CONNECTION_STRING" not in os.environ,
        reason="compute_day reads instrument config from the database",
    ),
]

GOLDEN_DAYS = [
    "2026-07-07", "2026-07-13", "2026-07-14", "2026-07-15",
    "2026-07-16", "2026-07-27", "2026-07-29", "2026-07-30",
]

# Bucket-valued fields: both paths land on exact multiples of bucket_size, so
# these compare exactly. `None` is meaningful (no poor extreme / no tail) and
# must round-trip as None, not as 0.
EXACT_FIELDS = (
    "poc", "vah", "val",
    "ib_high", "ib_low",
    "day_high", "day_low",
    "poor_high", "poor_low",
    "buying_tail", "selling_tail",
)

# v1 computed these ratios in Decimal (NUMERIC out of Postgres), v2 computes
# them in float, so the two can differ in the last bit of the double. Compared
# with a tight relative tolerance rather than pretending that is exact equality.
RATIO_FIELDS = ("extension_above", "extension_below")


def load_golden(date_str):
    with open(os.path.join(V1_GOLDEN_DIR, date_str + ".json"), encoding="utf-8") as handle:
        return json.load(handle)


def buckets_from_ranges(single_prints, bucket_size):
    """Expand [{"from": x, "to": y}] back to the flat bucket list v1 recorded.

    V2_SPEC 2.2 emits single prints as contiguous ranges; v1's `arr_single_tpo`
    was a flat list of bucket prices. Parity is about which buckets are single
    prints, not about the shape, so the ranges are expanded before comparing.
    """
    buckets = []
    for entry in single_prints:
        low = entry["from"]
        steps = int(round((entry["to"] - low) / bucket_size))
        buckets.extend(low + step * bucket_size for step in range(steps + 1))
    return sorted(buckets)


# --- the synthetic day: golden values, no network -----------------------------

def test_aggregate_archive_reads_the_synthetic_fixture():
    """The committed fixture parses to the profile its construction implies.

    tests/fixtures/synthetic_day.csv is built by scripts/make_synthetic_day.py,
    whose docstring carries the TPO profile it encodes, so the answers below are
    known by construction rather than by running the code under test.
    """
    with open(SYNTHETIC_CSV, newline="", encoding="utf-8") as handle:
        profile, period_ranges = ingest.aggregate_archive(
            handle, 25, 1800, SYNTHETIC_SESSION)

    # 100.0 is not a bucket here: prices start at 63000 and bucket_size is 25.
    assert min(profile) == 62950.0
    assert max(profile) == 63300.0
    # Every value is a set of period indices, and no period is out of range.
    for periods in profile.values():
        assert isinstance(periods, set)
        assert all(0 <= period < 48 for period in periods)
    # Period hi/lo are raw prices, not buckets.
    assert period_ranges[0][0] >= period_ranges[0][1]
    assert set(period_ranges) == {0, 1, 2, 3}


def test_synthetic_day_has_known_structures():
    """V2_SPEC PR 3 acceptance: fixture CSV -> known POC/VAH/VAL/IB."""
    with open(SYNTHETIC_CSV, newline="", encoding="utf-8") as handle:
        profile, period_ranges = ingest.aggregate_archive(
            handle, 25, 1800, SYNTHETIC_SESSION)

    row = ingest.compute_structures(
        profile, period_ranges,
        symbol="TESTUSDT", session_date=datetime.date(2026, 1, 1),
        bucket_size=25, ib_periods=2,
    )

    # Each of these is fixed by the fixture's construction; see
    # scripts/make_synthetic_day.py for the profile and the derivation.
    # POC: 63100 holds 4 TPOs, a unique maximum.
    assert row["poc"] == 63100.0
    # Value area: expanding from the POC captures 19 of 26 TPOs (73%) at
    # 63000-63175. Walked step by step in scripts/make_synthetic_day.py.
    assert row["vah"] == 63175.0
    assert row["val"] == 63000.0
    assert row["ib_high"] == 63150.0
    assert row["ib_low"] == 63000.0
    assert row["day_high"] == 63300.0
    assert row["day_low"] == 62950.0
    assert row["symbol"] == "TESTUSDT"
    assert row["session_date"] == datetime.date(2026, 1, 1)
    assert row["engine_version"] == ingest.ENGINE_VERSION


# --- aggregate_archive units --------------------------------------------------

def _csv_line(agg_id, price, qty, ts_micro, is_buyer_maker="False"):
    """One archive row: [0]=id [1]=price [2]=qty [3]=first [4]=last [5]=ts_micro
    [6]=is_buyer_maker [7]=is_best_match."""
    return "%d,%s,%s,%d,%d,%d,%s,True" % (
        agg_id, price, qty, agg_id, agg_id, ts_micro, is_buyer_maker)


def test_bucket_and_period_are_computed_from_the_configured_values():
    # 00:00:00.000001 -> period 0; 00:29:59 -> period 0; 00:30:00 -> period 1.
    rows = [
        _csv_line(1, "63012.34000000", "0.1", 1),
        _csv_line(2, "63037.99000000", "0.5", 1799 * 1_000_000),
        _csv_line(3, "62999.99000000", "0.5", 1800 * 1_000_000),
    ]

    profile, period_ranges = ingest.aggregate_archive(
        iter(rows), 25, 1800, SYNTHETIC_SESSION)

    assert profile == {63000.0: {0}, 63025.0: {0}, 62975.0: {1}}
    assert period_ranges == {0: (63037.99, 63012.34), 1: (62999.99, 62999.99)}


def test_period_width_comes_from_period_seconds():
    rows = [_csv_line(1, "63000.0", "1", 3599 * 1_000_000),
            _csv_line(2, "63000.0", "1", 3600 * 1_000_000)]

    _profile, hourly = ingest.aggregate_archive(
        iter(rows), 25, 3600, SYNTHETIC_SESSION)

    assert set(hourly) == {0, 1}


def test_bucket_size_is_not_hardcoded():
    rows = [_csv_line(1, "63010.0", "1", 0)]

    assert ingest.aggregate_archive(
        iter(rows), 10, 1800, SYNTHETIC_SESSION)[0] == {63010.0: {0}}
    assert ingest.aggregate_archive(
        iter(rows), 100, 1800, SYNTHETIC_SESSION)[0] == {63000.0: {0}}


def test_timestamps_are_microseconds_not_milliseconds():
    """The trap recorded in CURRENT_STATE 3b: the archive is microseconds.

    23:59:59 in microseconds is the last period of the day. Read as
    milliseconds the same number would fall a thousandfold further out.
    """
    rows = [_csv_line(1, "63000.0", "1", 86399 * 1_000_000)]

    _profile, period_ranges = ingest.aggregate_archive(
        iter(rows), 25, 1800, SYNTHETIC_SESSION)

    assert set(period_ranges) == {47}


def test_a_header_row_is_tolerated_but_later_junk_is_not():
    """Binance has shipped both headerless and headed archives."""
    header = "agg_trade_id,price,quantity,first,last,transact_time,is_buyer_maker,is_best_match"
    rows = [header, _csv_line(1, "63000.0", "1", 0)]

    profile, _ranges = ingest.aggregate_archive(iter(rows), 25, 1800, SYNTHETIC_SESSION)
    assert profile == {63000.0: {0}}

    with pytest.raises(ValueError):
        ingest.aggregate_archive(
            iter([_csv_line(1, "63000.0", "1", 0), "not,a,valid,row,at,all,x,y"]),
            25, 1800, SYNTHETIC_SESSION)


# --- the session window -------------------------------------------------------

def test_a_row_after_midnight_is_dropped_from_the_session():
    """A row outside the session window is excluded, not folded into a period.

    The two fixtures differ by exactly one row, timestamped 00:00:00.000001 on
    1970-01-02 at 99999.99 — outside the half-open window [session, session + 1
    day), which is what v1's WHERE clause enforced. Aggregating them for
    1970-01-01 must therefore give the same answer.

    Before the window was applied the period came from a modulo over
    seconds-since-epoch, which folded that row into period 0 and moved day_high
    from 63300 to 99975.
    """
    with open(SYNTHETIC_CSV, newline="", encoding="utf-8") as handle:
        expected, expected_ranges = ingest.aggregate_archive(
            handle, 25, 1800, SYNTHETIC_SESSION)

    with open(SYNTHETIC_STRAY_CSV, newline="", encoding="utf-8") as handle:
        actual, actual_ranges = ingest.aggregate_archive(
            handle, 25, 1800, SYNTHETIC_SESSION)

    assert actual == expected
    assert actual_ranges == expected_ranges
    # Specifically: the stray row's bucket is absent and the day's high stands.
    assert 99975.0 not in actual
    assert max(actual) == 63300.0


def test_the_stray_row_is_only_dropped_because_of_its_date():
    """Guard against the test passing for the wrong reason.

    Aggregated as 1970-01-02 — the day it actually belongs to — the same stray
    row is kept. That proves the exclusion is the window doing its job and not
    the row being malformed or silently unparseable.
    """
    with open(SYNTHETIC_STRAY_CSV, newline="", encoding="utf-8") as handle:
        profile, _ranges = ingest.aggregate_archive(
            handle, 25, 1800, datetime.date(1970, 1, 2))

    assert profile == {99975.0: {0}}


def test_the_session_window_is_half_open():
    """[session 00:00:00.000000, next session 00:00:00.000000).

    Four rows one microsecond apart around both edges: the last microsecond of
    the previous day and the first of the next are out; the first and last
    microseconds of the session itself are in.
    """
    day = 86_400 * 1_000_000
    rows = [
        _csv_line(1, "100.0", "1", day - 1),          # 1969-12-31 23:59:59.999999
        _csv_line(2, "200.0", "1", day),              # 1970-01-01 00:00:00.000000
        _csv_line(3, "300.0", "1", 2 * day - 1),      # 1970-01-01 23:59:59.999999
        _csv_line(4, "400.0", "1", 2 * day),          # 1970-01-02 00:00:00.000000
    ]

    profile, period_ranges = ingest.aggregate_archive(
        iter(rows), 100, 1800, datetime.date(1970, 1, 2))

    # Only rows 2 and 3 survive, and they land in the first and last period.
    assert profile == {200.0: {0}, 300.0: {47}}
    assert set(period_ranges) == {0, 47}


def test_periods_are_measured_from_the_session_start_not_from_the_epoch():
    """The period index is an offset into the session, for any session date."""
    day = 86_400 * 1_000_000
    rows = [_csv_line(1, "63000.0", "1", 20_000 * day + 1799 * 1_000_000),
            _csv_line(2, "63000.0", "1", 20_000 * day + 1800 * 1_000_000)]

    _profile, period_ranges = ingest.aggregate_archive(
        iter(rows), 25, 1800, SYNTHETIC_SESSION + datetime.timedelta(days=20_000))

    assert set(period_ranges) == {0, 1}


# --- detect_trend, refactored off SQL ----------------------------------------

def test_detect_trend_counts_violations_in_period_order():
    """Same arithmetic as the v1 SQL version, fed a dict instead of rows.

    Five periods, each (high, low). Lows fall once (period 2 -> 3), highs rise
    twice (0 -> 1 and 3 -> 4), over n-1 = 4 comparisons.
    """
    period_ranges = {
        0: (100.0, 90.0),
        1: (105.0, 95.0),
        2: (104.0, 96.0),
        3: (103.0, 92.0),
        4: (106.0, 93.0),
    }

    up_conf, down_conf = ingest.detect_trend(period_ranges)

    assert up_conf == 1 - 1 / 4
    assert down_conf == 1 - 2 / 4


def test_detect_trend_is_insensitive_to_dict_ordering():
    ordered = {0: (100.0, 90.0), 1: (101.0, 91.0), 2: (102.0, 92.0)}
    shuffled = {2: (102.0, 92.0), 0: (100.0, 90.0), 1: (101.0, 91.0)}

    assert ingest.detect_trend(shuffled) == ingest.detect_trend(ordered)


def test_detect_trend_still_refuses_fewer_than_two_periods():
    """The v1 guard against dividing by n-1 = 0."""
    with pytest.raises(ValueError):
        ingest.detect_trend({0: (100.0, 90.0)})


# --- single prints as contiguous ranges --------------------------------------

def test_single_prints_are_emitted_as_contiguous_ranges():
    buckets = [62650.0, 62675.0, 62700.0, 64275.0, 64300.0]

    ranges = ingest.single_print_ranges(buckets, 25)

    assert ranges == [
        {"from": 62650.0, "to": 62700.0},
        {"from": 64275.0, "to": 64300.0},
    ]


def test_an_isolated_single_print_is_a_range_of_one_bucket():
    assert ingest.single_print_ranges([63000.0], 25) == [
        {"from": 63000.0, "to": 63000.0}]
    assert ingest.single_print_ranges([], 25) == []


def test_contiguity_follows_bucket_size():
    buckets = [100.0, 110.0]

    assert len(ingest.single_print_ranges(buckets, 10)) == 1   # adjacent
    assert len(ingest.single_print_ranges(buckets, 5)) == 2    # a gap between


# --- unpublished session ------------------------------------------------------

class _Response:
    def __init__(self, status_code):
        self.status_code = status_code

    def iter_content(self, chunk_size=1):
        return iter(())

    def close(self):
        pass


@pytest.fixture
def stub_instrument(monkeypatch):
    """The seeded BTCUSDT config, without touching the database."""
    monkeypatch.setattr(ingest.db, "get_instrument", lambda symbol: {
        "symbol": symbol,
        "exchange": "binance-spot",
        "bucket_size": 25.0,
        "period_seconds": 1800,
        "ib_periods": 2,
        "archive_url_template": "https://example.invalid/{symbol}/{date}.zip",
        "active": True,
    })


def test_unpublished_date_raises_session_not_published(monkeypatch, stub_instrument):
    monkeypatch.setattr(ingest.requests, "get",
                        lambda url, **kwargs: _Response(404))
    written = []
    monkeypatch.setattr(ingest.db, "upsert_daily_levels", written.append)

    with pytest.raises(ingest.SessionNotPublished):
        ingest.compute_day("BTCUSDT", datetime.date(2026, 9, 11))

    assert written == []   # no partial row


def test_other_http_failures_raise_archive_unavailable(monkeypatch, stub_instrument):
    monkeypatch.setattr(ingest.requests, "get",
                        lambda url, **kwargs: _Response(503))
    written = []
    monkeypatch.setattr(ingest.db, "upsert_daily_levels", written.append)

    with pytest.raises(ingest.ArchiveUnavailable):
        ingest.compute_day("BTCUSDT", datetime.date(2026, 9, 11))

    assert written == []


def test_unknown_symbol_is_rejected_before_any_download(monkeypatch):
    monkeypatch.setattr(ingest.db, "get_instrument", lambda symbol: None)

    def fail(*args, **kwargs):
        raise AssertionError("must not download for an unknown symbol")

    monkeypatch.setattr(ingest.requests, "get", fail)

    with pytest.raises(ingest.UnknownInstrument):
        ingest.compute_day("NOPE", datetime.date(2026, 7, 27))


# --- memory -------------------------------------------------------------------

def _synthetic_rows(count, buckets=400, periods=48):
    """`count` archive lines walking a price range, spread across the day.

    Yielded lazily: the generator is the stream, so the file is never
    materialised anywhere and what tracemalloc sees is the accumulator plus the
    csv reader's own working set.
    """
    for i in range(count):
        price = 60000 + (i % (buckets * 25))
        ts_micro = ((i * 86400) // count) * 1_000_000
        yield "%d,%d.00000000,0.5,%d,%d,%d,False,True" % (i, price, i, i, ts_micro)


MEMORY_CEILING_BYTES = 256 * 1024 * 1024   # V2_SPEC D2


@pytest.mark.slow
@pytest.mark.skipif(IN_CI, reason="5M rows: runtime only, skipped in CI")
def test_aggregation_stays_under_the_memory_ceiling_on_a_five_million_row_day():
    """V2_SPEC PR 3: < 256 MB on a 5M-row day.

    Measured with tracemalloc, which counts Python allocations rather than RSS;
    the RSS figure is in the PR description. The accumulator is bounded by
    buckets x periods, so peak must not scale with row count.
    """
    tracemalloc.start()
    try:
        profile, period_ranges = ingest.aggregate_archive(
            _synthetic_rows(5_000_000), 25, 1800, SYNTHETIC_SESSION)
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert peak < MEMORY_CEILING_BYTES, "peak %.1f MB" % (peak / 1024 / 1024)
    # Sanity: the whole day really was aggregated.
    assert len(period_ranges) == 48
    assert len(profile) == 400


def test_accumulator_does_not_grow_with_row_count():
    """The property behind the ceiling: state is bounded by buckets x periods."""
    small = ingest.aggregate_archive(
        _synthetic_rows(20_000), 25, 1800, SYNTHETIC_SESSION)
    large = ingest.aggregate_archive(
        _synthetic_rows(200_000), 25, 1800, SYNTHETIC_SESSION)

    assert len(small[0]) == len(large[0]) == 400
    assert len(small[1]) == len(large[1]) == 48


# --- parity with the v1 Postgres path ----------------------------------------

@pytest.fixture
def captured_upsert(monkeypatch):
    """compute_day's write, captured in a list instead of committed.

    `db.upsert_daily_levels` commits, and `CONNECTION_STRING` in practice points
    at the live database — so letting these tests run it would write real
    `daily_levels` rows as a side effect of asserting parity. What parity needs
    is the row compute_day produced, which is exactly what the capture holds.

    `db.get_instrument` is deliberately left alone: reading the seeded
    instrument config from the real database is part of what these tests cover,
    and a read commits nothing.
    """
    rows = []
    monkeypatch.setattr(ingest.db, "upsert_daily_levels", rows.append)
    return rows


@pytest.mark.parametrize("date_str", GOLDEN_DAYS)
@pytest.mark.network
@pytest.mark.skipif(IN_CI, reason="downloads from data.binance.vision; runs locally")
@pytest.mark.skipif("CONNECTION_STRING" not in os.environ,
                    reason="compute_day reads instrument config from the database")
def test_compute_day_reproduces_the_v1_golden_day(date_str, captured_upsert):
    """V2_SPEC PR 3 acceptance: the in-memory path reproduces the SQL path.

    Exact equality on day_type and reason is full parity for detect_trend (the
    decision recorded in V2_SPEC PR 3 acceptance): up_conf/down_conf reach the
    output only through the Trend and Directional branches of
    classify_day_type, both of which embed the value in `reason`.
    """
    golden = load_golden(date_str)
    session_date = datetime.date.fromisoformat(date_str)

    row = ingest.compute_day("BTCUSDT", session_date)

    assert row["day_type"] == golden["day_type"]
    assert row["reason"] == golden["reason"]
    for field in EXACT_FIELDS:
        assert row[field] == golden[field], field
    for field in RATIO_FIELDS:
        assert row[field] == pytest.approx(golden[field], rel=1e-12), field

    # Same single prints, different shape (ranges, not a flat list).
    bucket_size = db.get_instrument("BTCUSDT")["bucket_size"]
    assert buckets_from_ranges(row["single_prints"], bucket_size) == \
        sorted(golden["arr_single_tpo"])

    # And that row — exactly one, not a partial write and not a retry — is what
    # compute_day handed to storage, carrying the current engine version.
    assert len(captured_upsert) == 1
    stored = captured_upsert[0]
    assert stored is row
    assert stored["engine_version"] == ingest.ENGINE_VERSION
    assert stored["day_type"] == golden["day_type"]
    assert stored["poc"] == golden["poc"]


@pytest.mark.network
@pytest.mark.skipif(IN_CI, reason="downloads from data.binance.vision; runs locally")
@pytest.mark.skipif("CONNECTION_STRING" not in os.environ,
                    reason="compute_day reads instrument config from the database")
def test_profile_histogram_is_persisted_as_bucket_to_tpo_count(captured_upsert):
    """The JSONB `profile` composites will merge (V2_SPEC 2.2)."""
    session_date = datetime.date(2026, 7, 27)
    golden = load_golden("2026-07-27")

    row = ingest.compute_day("BTCUSDT", session_date)

    # The histogram under test is the one handed to storage, not a copy.
    assert captured_upsert == [row]
    profile = row["profile"]
    assert all(isinstance(key, str) for key in profile)
    assert all(isinstance(value, int) and value >= 1 for value in profile.values())
    # The extremes of the histogram are the day's extremes.
    prices = sorted(float(key) for key in profile)
    assert prices[0] == golden["day_low"]
    assert prices[-1] == golden["day_high"]
    # The POC is the bucket with the most TPOs.
    assert profile[str(int(golden["poc"]))] == max(profile.values())
