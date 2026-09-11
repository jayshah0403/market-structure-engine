"""Engine: archive -> in-memory profile -> detectors -> a daily_levels row.

`compute_day(symbol, session_date)` is the entry point (V2_SPEC PR 3). It reads
every parameter it needs from the `instruments` row — bucket size, period width,
IB length, archive URL — so no symbol, bucket, period or URL is written down
here (V2_SPEC section 4, "Config").

Raw ticks are never stored. The daily archive is streamed once, row by row, into
an accumulator bounded by buckets x periods, and then discarded; what persists is
the computed row. The v1 Postgres-backed `get_profile_grid` / `compute_profile`
are gone with the table they read.

No FastAPI import here, and no HTTP status codes: failures raise the engine's own
exceptions and the API layer decides what they mean (V2_SPEC section 4, "Errors").
"""

import calendar
import contextlib
import csv
import io
import math
import string
import tempfile
import time
import zipfile
from datetime import date as _date

import requests

import db

# Bump on any detector change; stale daily_levels rows then recompute lazily
# (V2_SPEC section 4, "Versioning").
ENGINE_VERSION = 1

# Connect and read timeout for the archive download, in seconds. Not instrument
# config: it is a transport tuning knob. This is the bound that actually limits
# how long a cold compute can run — `requests` waits forever without it, and a
# server that accepts the connection and then stops sending would hold an API
# compute slot open indefinitely (V2_SPEC PR 4, "Rules"). It caps the wait for
# headers and the gap between chunks, not total transfer time.
ARCHIVE_TIMEOUT_SECONDS = 60

# Total wall-clock bound on one archive fetch, in seconds. The timeout above is
# applied by `requests` to each individual read, so a server trickling one chunk
# just inside that window never trips it and holds an API compute slot open for
# as long as it likes. This is the bound on the whole transfer, and it is what
# makes "a cold compute cannot run indefinitely" true rather than nearly true.
# 300 s is deliberately generous: a real day's archive is 5-20 MB and completes
# in about 20 s, so this only fires on a link that is pathological rather than
# merely slow (V2_SPEC PR 4, "Rules").
ARCHIVE_DEADLINE_SECONDS = 300

# Streamed to disk in chunks this size, so neither the zip nor the CSV is ever
# held in memory whole.
DOWNLOAD_CHUNK_BYTES = 1024 * 1024

SECONDS_PER_DAY = 86400
MICROSECONDS_PER_SECOND = 1_000_000


class EngineError(Exception):
    """Base for every failure the engine reports. The API layer maps these to
    status codes; the engine itself knows nothing about HTTP."""


class SessionNotPublished(EngineError):
    """The archive for that session does not exist yet (published ~02:00 UTC
    the following day)."""


class ArchiveUnavailable(EngineError):
    """The archive should exist but could not be fetched or read."""


class UnknownInstrument(EngineError):
    """No `instruments` row for that symbol."""


class NoTradeData(EngineError, ValueError):
    """The archive was read but held no usable trades.

    Also a ValueError, so v1 callers that caught the old empty-profile
    ValueError keep working.
    """


letters = string.ascii_uppercase + string.ascii_lowercase


def archive_url(instrument, session_date):
    """The download URL for one session, from the instrument's own template."""
    return instrument["archive_url_template"].format(
        symbol=instrument["symbol"], date=session_date.isoformat())


@contextlib.contextmanager
def open_archive_csv(url):
    """Yield the archive's single CSV as a text stream.

    The response is streamed to a temporary file rather than into memory: zipfile
    needs a seekable source, and a 5M-row day is a ~75 MB zip around a ~430 MB
    CSV, neither of which may sit in the 256 MB budget (V2_SPEC D2). The temp
    file is deleted when the context closes.
    """
    # Started before the request, so the deadline covers connecting and waiting
    # for headers as well as the transfer itself.
    deadline = time.monotonic() + ARCHIVE_DEADLINE_SECONDS

    try:
        response = requests.get(url, stream=True, timeout=ARCHIVE_TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        raise ArchiveUnavailable("%s: %s" % (url, exc)) from exc

    try:
        if response.status_code == 404:
            raise SessionNotPublished(url)
        if response.status_code != 200:
            raise ArchiveUnavailable(
                "%s returned HTTP %d" % (url, response.status_code))

        with tempfile.TemporaryFile() as spool:
            try:
                for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                    if time.monotonic() > deadline:
                        raise ArchiveUnavailable(
                            "%s exceeded the %d s download deadline"
                            % (url, ARCHIVE_DEADLINE_SECONDS))
                    spool.write(chunk)
            except requests.RequestException as exc:
                raise ArchiveUnavailable("%s: %s" % (url, exc)) from exc
            spool.seek(0)

            try:
                archive = zipfile.ZipFile(spool)
            except zipfile.BadZipFile as exc:
                raise ArchiveUnavailable("%s is not a zip: %s" % (url, exc)) from exc

            with archive:
                members = archive.namelist()
                if len(members) != 1:
                    raise ArchiveUnavailable(
                        "%s holds %d files, expected 1" % (url, len(members)))
                with archive.open(members[0]) as member:
                    # newline="" so the csv module does its own line handling.
                    yield io.TextIOWrapper(member, encoding="utf-8", newline="")
    finally:
        response.close()


def aggregate_archive(csv_stream, bucket_size, period_seconds, session_date):
    """One streaming pass over the archive CSV, windowed to one session.

    Returns `(profile, period_ranges)`:
      * `profile` — `{bucket: {periods touched}}`, the TPO profile.
      * `period_ranges` — `{period: (high, low)}` of **raw prices**, which is
        what one-timeframing is measured on (the v1 SQL took MAX/MIN of price,
        not of bucket).

    `csv_stream` is any iterable of CSV lines — a `TextIOWrapper` over the zip
    member in production, a list or generator in tests. Rows are consumed one at
    a time and dropped, so memory is bounded by buckets x periods however long
    the file is.

    Archive columns are positional and headerless (V2_SPEC PR 3):
    `[0]=agg_trade_id [1]=price [2]=quantity [5]=timestamp_micro
    [6]=is_buyer_maker`. The timestamp is **microseconds** — the trap recorded in
    CURRENT_STATE 3b, where the retired REST path used milliseconds.

    `session_date` bounds the pass to the half-open window
    `[session_date 00:00 UTC, session_date + 1 day 00:00 UTC)`; rows outside it
    are dropped. This is v1's `WHERE` clause, restored. Without a date the
    period could only be derived by folding the timestamp with a modulo over
    seconds-since-epoch, which silently relabelled an out-of-day row as a period
    of *this* day instead of excluding it — the divergence from v1 recorded as
    ambiguity 4 on the PR. The period is now an offset into the session, so it
    no longer depends on the archive file happening to hold exactly one day.
    """
    bucket_size = float(bucket_size)
    period_seconds = int(period_seconds)
    # calendar.timegm reads the tuple as UTC; datetime.timestamp() would apply
    # the local zone, which would shift the window by the offset of whatever
    # machine ran it.
    session_start_micro = (
        calendar.timegm(session_date.timetuple()) * MICROSECONDS_PER_SECOND)
    session_end_micro = session_start_micro + SECONDS_PER_DAY * MICROSECONDS_PER_SECOND
    profile = {}
    period_ranges = {}

    for row_number, row in enumerate(csv.reader(csv_stream)):
        try:
            price = float(row[1])
            ts_micro = int(row[5])
        except (IndexError, ValueError):
            # Binance has shipped both headerless and headed archives; tolerate a
            # header, but never silently skip a bad row in the body.
            if row_number == 0:
                continue
            raise ValueError(
                "malformed archive row %d: %r" % (row_number + 1, row))

        # Half-open, so a trade at exactly the next midnight belongs to the next
        # session and is counted once, there, not twice.
        if not session_start_micro <= ts_micro < session_end_micro:
            continue

        bucket = math.floor(price / bucket_size) * bucket_size
        seconds_into_session = (
            (ts_micro - session_start_micro) // MICROSECONDS_PER_SECOND)
        period = seconds_into_session // period_seconds

        periods = profile.get(bucket)
        if periods is None:
            profile[bucket] = {period}
        else:
            periods.add(period)

        current = period_ranges.get(period)
        if current is None:
            period_ranges[period] = (price, price)
        elif price > current[0]:
            period_ranges[period] = (price, current[1])
        elif price < current[1]:
            period_ranges[period] = (current[0], price)

    return profile, period_ranges


def single_print_ranges(buckets, bucket_size):
    """Single-print buckets as contiguous ranges `[{"from": x, "to": y}]`.

    V2_SPEC 2.2: ranges, not v1's flat bucket list, so a consumer can tell one
    single-print zone from three. Adjacency is measured in whole buckets, so the
    step count is rounded rather than compared as floats.
    """
    bucket_size = float(bucket_size)
    ranges = []
    for bucket in sorted(float(value) for value in buckets):
        if ranges and round((bucket - ranges[-1]["to"]) / bucket_size) == 1:
            ranges[-1]["to"] = bucket
        else:
            ranges.append({"from": bucket, "to": bucket})
    return ranges


def _bucket_key(bucket):
    """JSON object keys must be strings: 63225.0 -> "63225" (V2_SPEC 2.2)."""
    return str(int(bucket)) if float(bucket).is_integer() else repr(float(bucket))


def compute_day(symbol, session_date):
    """Compute one session from the archive and persist it. Returns the row.

    Nothing is written unless every step succeeds, so a failure never leaves a
    partial `daily_levels` row.
    """
    instrument = db.get_instrument(symbol)
    if instrument is None:
        raise UnknownInstrument(symbol)

    with open_archive_csv(archive_url(instrument, session_date)) as csv_stream:
        profile, period_ranges = aggregate_archive(
            csv_stream, instrument["bucket_size"], instrument["period_seconds"],
            session_date)

    row = compute_structures(
        profile, period_ranges,
        symbol=symbol, session_date=session_date,
        bucket_size=instrument["bucket_size"],
        ib_periods=instrument["ib_periods"],
    )
    db.upsert_daily_levels(row)
    return row

def compute_poc(levels, counts):
    poc = 0
    max_tpo = 0
    range_mid = (levels[-1] + levels[0]) / 2
    for price, tpo in counts.items():
        if tpo > max_tpo:
            max_tpo = tpo
            poc = price
        elif tpo == max_tpo and abs(price - range_mid) < abs(poc - range_mid):
            max_tpo = tpo
            poc = price
    return poc

def compute_ib(profile, ib_periods=2):
    # ib_periods comes from the instruments row; the default reproduces v1's
    # hardcoded "periods 0 or 1" exactly, so the detector's logic is unchanged.
    ib_window = range(ib_periods)
    arr_ib = []
    arr_single_tpo = []
    for price, periods in profile.items():
        if len(periods) == 1:
            arr_single_tpo.append(price)
        if any(period in periods for period in ib_window):
            arr_ib.append(price)
    return max(arr_ib), min(arr_ib), arr_single_tpo

def compute_structures(profile, period_ranges, symbol, session_date,
                       bucket_size, ib_periods):
    """Run every detector over one in-memory profile; return the daily_levels row.

    Pure: no I/O, no clock, no database. The dict it returns is keyed exactly on
    `daily_levels` columns so it can go straight to `db.upsert_daily_levels`
    (`computed_at` is left to the schema default, `composite_id` to PR 6).

    v1 took `start_timestamp` and fetched the profile itself; the detectors in
    between are unchanged.
    """
    levels = sorted(profile.keys())
    if not levels:
        raise NoTradeData("no trades for %s on %s" % (symbol, session_date))
    counts = {level: len(periods) for level, periods in profile.items()}
    poc = compute_poc(levels, counts)

    ib_high, ib_low, arr_single_tpo = compute_ib(profile, ib_periods)
    is_dd = detect_double_distribution_split(segment_profile(levels, counts, 1))
    up_conf, down_conf = detect_trend(period_ranges)
    day_type, reason = classify_day_type(session_date, ib_high, ib_low,
                                          max(levels), min(levels), is_dd, up_conf, down_conf)
    extension_above, extension_below = (max(levels) - ib_high) / (ib_high - ib_low), (ib_low - min(levels)) / (ib_high - ib_low)
    vah, val = compute_value_area(poc, levels, counts)
    day_high, day_low = max(levels), min(levels)
    # A poor extreme is the day's own extreme revisited in >= 2 periods.
    poor_high = float(day_high) if counts[day_high] >= 2 else None
    poor_low = float(day_low) if counts[day_low] >= 2 else None
    # A tail is a single-print run running all the way to a day extreme. With no
    # single prints at all there is no tail, and min()/max() must not be called.
    if arr_single_tpo:
        buying_tail = float(min(arr_single_tpo)) if min(arr_single_tpo) == day_low else None
        selling_tail = float(max(arr_single_tpo)) if max(arr_single_tpo) == day_high else None
    else:
        buying_tail = selling_tail = None
    return {
        "symbol": symbol,
        "session_date": session_date,
        "engine_version": ENGINE_VERSION,
        "day_type": day_type,
        "reason": reason,
        "poc": float(poc), "vah": float(vah), "val": float(val),
        "ib_high": float(ib_high), "ib_low": float(ib_low),
        "poor_high": poor_high,
        "poor_low": poor_low,
        "extension_above": extension_above,
        "extension_below": extension_below,
        "up_conf": up_conf,
        "down_conf": down_conf,
        "buying_tail": buying_tail,
        "selling_tail": selling_tail,
        "day_low": float(day_low),
        "day_high": float(day_high),
        # v1's flat arr_single_tpo becomes contiguous ranges (V2_SPEC 2.2).
        "single_prints": single_print_ranges(arr_single_tpo, bucket_size),
        # bucket -> TPO count, ascending. This is what composites merge (PR 6).
        "profile": {_bucket_key(level): counts[level] for level in levels},
    }

def compute_value_area(poc, levels, counts):
    i_poc = levels.index(poc)
    captured = counts[poc]
    target = sum(counts.values()) * 0.7
    higher_i = i_poc + 1
    lower_i = i_poc - 1
    while captured < target:
        if higher_i < len(levels) and (not lower_i >= 0 or counts[levels[higher_i]] >= counts[levels[lower_i]]):
            captured += counts[levels[higher_i]]
            higher_i += 1
        elif lower_i >= 0:
            captured += counts[levels[lower_i]]
            lower_i -= 1
        else:
            break
    return levels[higher_i - 1], levels[lower_i + 1]

def segment_profile(levels, counts, thin_max=1):
    segmented_profile = []
    thick_run = []
    thin_run = []
    for price in sorted(levels, reverse=True):
        if counts[price] >= 2:
            if len(thin_run) != 0:
                segmented_profile.append(["thin", thin_run])
            thin_run = []
            thick_run.append(int(price))
        elif counts[price] <= thin_max:
            if len(thick_run) != 0:
                segmented_profile.append(["thick", thick_run])
            thick_run = []
            thin_run.append(int(price))
    if len(thin_run) != 0:
        segmented_profile.append(["thin", thin_run])
    if len(thick_run) != 0:
        segmented_profile.append(["thick", thick_run])
    return segmented_profile

def detect_double_distribution_split(segments, min_thick_levels=3):
    segmented_profile = segments
    for i in range(len(segmented_profile) - 2):
        a, b, c = segments[i], segments[i + 1], segments[i + 2]
        if a[0] == 'thick' and len(a[1]) >= min_thick_levels \
            and b[0] == 'thin' \
            and c[0] == 'thick' and len(c[1]) >= min_thick_levels:
                return True
    return False

def detect_trend(period_ranges):
    """One-timeframing confidence from `{period: (high, low)}`.

    Was a second SQL aggregation over `trades`; now takes the period ranges
    `aggregate_archive` already collected. The arithmetic is v1's, untouched:
    sorting by period reproduces the old `ORDER BY period`, and the old row
    layout `(period, high, low)` becomes the tuple `(high, low)`, so index 2
    (low) becomes index 1 and index 1 (high) becomes index 0. Periods with no
    trades are absent in both versions.
    """
    periods = [period_ranges[period] for period in sorted(period_ranges)]
    if len(periods) < 2:
        raise ValueError(f"Insufficient period data: {len(periods)} period(s)")
    uptrend_violation = 0
    downtrend_violation = 0
    for i in range(1, len(periods)):
        if periods[i - 1][1] > periods[i][1]:
            uptrend_violation += 1
        if periods[i - 1][0] < periods[i][0]:
            downtrend_violation += 1

    return (1 - (uptrend_violation / (len(periods) - 1)), 1 - (downtrend_violation / (len(periods) - 1)))

def classify_day_type(start_timestamp, ib_high, ib_low, day_high, day_low,
                      is_double_dist, uptrend_conf, downtrend_conf):

    ib_range = ib_high - ib_low
    ext_up = day_high - ib_high
    ext_down = ib_low - day_low
    ext_up_mult = ext_up / ib_range      # extension as multiples of IB
    ext_down_mult = ext_down / ib_range
    SMALL = 0.4
    MEANINGFUL = 0.6
    TREND_THRESH = 0.8
    if ext_up_mult < SMALL and ext_down_mult < SMALL:
        return ("Non-Trend", f"Minimal extension both sides ({ext_up_mult:.1f}x up, {ext_down_mult:.1f}x down); no directional conviction")
    elif ext_up_mult > MEANINGFUL and ext_down_mult > MEANINGFUL:
        return ("Neutral", f"Extended both sides ({ext_up_mult:.1f}x up, {ext_down_mult:.1f}x down) — two-sided, responsive")
    elif is_double_dist:
        return ("Double-Distribution Trend", "Two distributions separated by single prints")
    elif downtrend_conf > TREND_THRESH:
        return ("Trend (down)", f"One-timeframe selling, {downtrend_conf:.2f} confirmation")
    elif uptrend_conf > TREND_THRESH:
        return ("Trend (up)", f"One-timeframe buying, {uptrend_conf:.2f} confirmation")
    elif ext_down_mult > MEANINGFUL or ext_up_mult > MEANINGFUL:
        direction = "down" if ext_down_mult > ext_up_mult else "up"
        mult = max(ext_down_mult, ext_up_mult)
        return ("Directional (unclassified)",
            f"Extended {mult:.1f}x IB to the {direction}; one-timeframing "
            f"{max(uptrend_conf, downtrend_conf):.2f} — directional but not a clean trend")
    else:
        return ("Normal", "Balanced day, price worked within the initial balance")

def generate_report(structures):
    lines = []
    lines.append("═" * 40)
    lines.append(f"{structures['symbol']} — Session Report, {structures['session_date']}")
    lines.append("═" * 40)
    lines.append(f"Day Type   : {structures['day_type']}")
    lines.append(f"Reason     : {structures['reason']}")
    lines.append(f"VALUE")
    lines.append(f"POC         : {structures['poc']}")
    lines.append(f"Value Area  : {structures['val']} - {structures['vah']}")
    lines.append(f"Initial Balance: {structures['ib_low']} - {structures['ib_high']}")
    lines.append(f"STRUCTURE")
    lines.append(f"Extension: {structures['extension_above']:.2f}/{structures['extension_below']:.2f}")
    lines.append(f"Buying Tail: {structures['buying_tail'] or 'None'} ")
    lines.append(f"Selling Tail: {structures['selling_tail'] or 'None'}")
    lines.append(f"Poor Low/High: {structures['poor_low'] or 'None'} / {structures['poor_high'] or 'None'}")
    lines.append("=" * 40)

    return "\n".join(lines)


if __name__ == "__main__":
    # Symbol and date are arguments, not literals: the engine hardcodes neither.
    #   python ingest.py BTCUSDT 2026-07-27
    import sys

    if len(sys.argv) != 3:
        raise SystemExit("usage: python ingest.py <SYMBOL> <YYYY-MM-DD>")
    print(generate_report(compute_day(sys.argv[1], _date.fromisoformat(sys.argv[2]))))