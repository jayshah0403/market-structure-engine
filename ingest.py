import requests
import psycopg2
import os
# pyrefly: ignore [missing-import]
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from collections import defaultdict
import string
from datetime import datetime, timezone


conn = None
cur = None

load_dotenv()

def get_cursor():
    global conn, cur
    if cur is None:
        conn = psycopg2.connect(os.environ["CONNECTION_STRING"])
        cur = conn.cursor()
    return cur

letters = string.ascii_uppercase + string.ascii_lowercase

start_timestamp = 1783382400000
end_timestamp = start_timestamp + 86400000

def get_profile_grid(start_ts_ms):
    cur = get_cursor()
    end_ts_ms = start_ts_ms + 86400000
    cur.execute("""
        SELECT FLOOR(price / 25) * 25 AS price_bucket,
               FLOOR(EXTRACT(EPOCH FROM (ts AT TIME ZONE 'UTC')::time) / 1800) AS period,
               COUNT(*) AS trade_count,
               SUM(quantity) AS volume
        FROM trades
        WHERE ts >= to_timestamp(%s / 1000.0) AND ts < to_timestamp(%s / 1000.0)
        GROUP BY price_bucket, period
        ORDER BY price_bucket, period;
    """, (start_ts_ms, end_ts_ms))
    rows = cur.fetchall()
    return rows

def fetchDayRecords(date):
    cur = get_cursor()
    cur.execute("SELECT id FROM instruments WHERE symbol = %s", ("BTCUSDT",))
    instrument_id = cur.fetchone()[0]
    start_timestamp = date
    end_time = date + 86400000
    first_trade = requests.get("https://api.binance.com/api/v3/aggTrades", params={"symbol": "BTCUSDT", "startTime": start_timestamp, "limit": 1}).json()
    from_id = first_trade[0]['a']
    while True:
        response = requests.get("https://api.binance.com/api/v3/aggTrades", params={"symbol": "BTCUSDT", "fromId": from_id, "limit": 1000}).json()
        if len(response) == 0:
            break
        batch = [t for t in response if t['T'] <= end_time]
        if batch:
            rows = [(t['a'], instrument_id, t['p'], t['q'], t['T'], t['m']) for t in batch]
            execute_values(cur, "INSERT INTO trades (agg_trade_id, instrument_id, price, quantity, ts, is_buyer_maker) "
                "VALUES %s ON CONFLICT (agg_trade_id) DO NOTHING",
                rows,
                template="(%s, %s, %s, %s, to_timestamp(%s / 1000.0), %s)")
            conn.commit()

        if response[-1]['T'] > end_time:
            break
        from_id = response[-1]['a'] + 1
        if len(response) < 1000:
            break

def compute_profile(start_timestamp):
    lst = get_profile_grid(start_timestamp)
    profile = defaultdict(list)
    for price_bucket, period, trade_count, volume in lst:
        profile[price_bucket].append(period)
    return profile

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

def compute_ib(profile):
    arr_ib = []
    arr_single_tpo = []
    for price, periods in profile.items():
        if len(periods) == 1:
            arr_single_tpo.append(price)
        if (1 in periods or 0 in periods):
            arr_ib.append(price)
    return max(arr_ib), min(arr_ib), arr_single_tpo

def compute_structures(start_timestamp):
    profile = compute_profile(start_timestamp)
    levels = sorted(profile.keys())
    if not levels:
        raise ValueError(f"No trade data for {start_timestamp}")
    counts = {level: len(periods) for level, periods in profile.items()}
    poc = compute_poc(levels, counts)

    ib_high, ib_low, arr_single_tpo = compute_ib(profile)
    is_dd = detect_double_distribution_split(segment_profile(levels, counts, 1))
    up_conf, down_conf = detect_trend(start_timestamp)
    day_type, reason = classify_day_type(start_timestamp, ib_high, ib_low,
                                          max(levels), min(levels), is_dd, up_conf, down_conf)
    extension_above, extension_below = (max(levels) - ib_high) / (ib_high - ib_low), (ib_low - min(levels)) / (ib_high - ib_low)
    vah, val = compute_value_area(poc, levels, counts)
    return {
        "date": datetime.fromtimestamp(start_timestamp / 1000, timezone.utc).strftime("%Y-%m-%d"),
        "day_type": day_type,
        "reason": reason,
        "poc": float(poc), "vah": float(vah), "val": float(val),
        "ib_high": float(ib_high), "ib_low": float(ib_low),
        "arr_single_tpo": arr_single_tpo,
        "poor_high": float(min(levels)) if counts[min(levels)] >= 2 else None,
        "poor_low": float(max(levels)) if counts[max(levels)] >= 2 else None,
        "extension_above": extension_above,
        "extension_below": extension_below,
        "buying_tail": float(min(arr_single_tpo)) if min(arr_single_tpo) == min(levels) else None,
        "selling_tail": float(max(arr_single_tpo)) if max(arr_single_tpo) == max(levels) else None,
        "day_low": float(min(levels)),
        "day_high": float(max(levels))
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

def detect_trend(start_timestamp):
    cur = get_cursor()
    cur.execute('''SELECT
    FLOOR(EXTRACT(EPOCH FROM (ts AT TIME ZONE 'UTC')::time) / 1800) AS period,
    MAX(price) AS period_high,
    MIN(price) AS period_low
    FROM trades
    WHERE ts >= to_timestamp(%s / 1000.0) AND ts < to_timestamp(%s / 1000.0)
    GROUP BY period
    ORDER BY period''', (start_timestamp, start_timestamp + 86400000))
    periods = cur.fetchall()
    if len(periods) < 2:
        raise ValueError(f"Insufficient period data for {start_timestamp}")
    uptrend_violation = 0
    downtrend_violation = 0
    for i in range(1, len(periods)):
        if periods[i - 1][2] > periods[i][2]:
            uptrend_violation += 1
        if periods[i - 1][1] < periods[i][1]:
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
    lines.append(f"BTC/USDT — Session Report, {structures['date']}")
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
    report = generate_report(compute_structures(start_timestamp))
    print(report)