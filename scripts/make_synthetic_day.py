"""Generate tests/fixtures/synthetic_day.csv — the no-network golden fixture.

Committed so the fixture's construction is auditable rather than magic: the
expected POC/VA/IB in tests/test_compute.py are derived by hand from the TPO
profile below, and this script is what turns that profile into archive-shaped
CSV rows.

    python scripts/make_synthetic_day.py

The profile (bucket -> periods touched), with bucket_size 25 and
period_seconds 1800, chosen so that every structure has one obvious answer:

    bucket   periods        TPOs
    63300    3              1
    63275    3              1
    63250    3              1
    63225    3              1
    63200    3              1
    63175    3              1
    63150    0, 3           2
    63125    0, 3           2
    63100    0, 1, 2, 3     4   <- POC, unique maximum
    63075    0, 1, 2        3
    63050    0, 1, 2        3
    63025    1, 2           2
    63000    1, 2           2
    62975    2              1
    62950    2              1                 total TPOs = 26

  day_high / day_low      63300 / 62950   (max / min bucket)
  IB (periods 0 and 1)    63150 / 63000
  POC                     63100           (4 TPOs, no tie to break)
  Value area              63000 - 63175   (expansion below captures 19 of 26
                                           TPOs = 73% >= 70%; see the test)
  single prints           62950-62975 and 63175-63300 (two ranges)
  extensions              1.00x up, 0.33x down of a 150-wide IB
  one-timeframing         2 of 3 lows violated, 1 of 3 highs violated

Each (bucket, period) cell emits REPEATS x len(PRICE_OFFSETS) trades at prices
inside the bucket — including bucket+0.01 and bucket+24.99, so the fixture also
proves that flooring puts both edges of a bucket in the same bucket. Rows come
out in timestamp order, like a real archive.

Timestamps are offsets from epoch midnight, so **the fixture's session date is
1970-01-01** — that is the date aggregate_archive has to be given to place these
rows inside the session window.

A second fixture, synthetic_day_with_stray_row.csv, is the same 312 rows plus
one row timestamped 00:00:00.000001 on 1970-01-02 at a price far outside the
day's range. It falls outside the half-open window [session, session + 1 day),
so aggregate_archive must drop it and return exactly the first fixture's
profile. Folded in by a modulo instead — what the code did before the window was
applied — it would land in period 0 and move day_high from 63300 to 99975, so a
regression fails loudly rather than subtly.
"""

import os

PROFILE = {
    63300: [3],
    63275: [3],
    63250: [3],
    63225: [3],
    63200: [3],
    63175: [3],
    63150: [0, 3],
    63125: [0, 3],
    63100: [0, 1, 2, 3],
    63075: [0, 1, 2],
    63050: [0, 1, 2],
    63025: [1, 2],
    63000: [1, 2],
    62975: [2],
    62950: [2],
}

PERIOD_SECONDS = 1800
PRICE_OFFSETS = ("0.01", "8.33", "16.66", "24.99")
REPEATS = 3

FIXTURE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tests", "fixtures",
)
OUT_PATH = os.path.join(FIXTURE_DIR, "synthetic_day.csv")
STRAY_OUT_PATH = os.path.join(FIXTURE_DIR, "synthetic_day_with_stray_row.csv")

# One microsecond past midnight of the following day: the first timestamp the
# half-open window [session, session + 1 day) must exclude.
STRAY_TS_MICRO = 86_400 * 1_000_000 + 1
# Far outside the day's 62950-63300 range, so a wrongly included row cannot be
# mistaken for a rounding difference.
STRAY_PRICE = "99999.99000000"
STRAY_ROW = "4000000313,%s,0.05000000,4000000313,4000000313,%d,False,True" % (
    STRAY_PRICE, STRAY_TS_MICRO)


def rows():
    cells = [(period, bucket)
             for bucket, periods in PROFILE.items()
             for period in periods]
    cells.sort()   # chronological: period ascending, then price

    trades = []
    for period, bucket in cells:
        for repeat in range(REPEATS):
            for offset in PRICE_OFFSETS:
                trades.append((period, "%.8f" % (bucket + float(offset))))

    # Spread each period's trades evenly inside its 1800-second window so no two
    # rows share a timestamp and none can drift into the next period.
    per_period = {}
    for period, price in trades:
        per_period.setdefault(period, []).append(price)

    agg_id = 4000000000
    for period in sorted(per_period):
        prices = per_period[period]
        step_us = (PERIOD_SECONDS * 1_000_000) // (len(prices) + 1)
        base_us = period * PERIOD_SECONDS * 1_000_000
        for index, price in enumerate(prices, start=1):
            ts_micro = base_us + index * step_us
            agg_id += 1
            # [0]=agg_trade_id [1]=price [2]=quantity [3]=first_id [4]=last_id
            # [5]=timestamp_micro [6]=is_buyer_maker [7]=is_best_match
            yield "%d,%s,0.05000000,%d,%d,%d,%s,True" % (
                agg_id, price, agg_id, agg_id, ts_micro,
                "True" if index % 2 else "False")


def write(path, lines):
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        for line in lines:
            handle.write(line + "\n")
    print("wrote %s (%d rows)" % (os.path.relpath(path), len(lines)))


def main():
    lines = list(rows())
    write(OUT_PATH, lines)
    write(STRAY_OUT_PATH, lines + [STRAY_ROW])


if __name__ == "__main__":
    main()
