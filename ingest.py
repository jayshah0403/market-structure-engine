from psycopg2 import extras
from psycopg2 import extras
from psycopg2 import extras
from psycopg2 import extras
from psycopg2 import extras
from psycopg2 import extras
from psycopg2 import extras
from json import decoder
from collections import abc
from collections import abc
from requests import sessions
import subprocess
import subprocess
import requests
import psycopg2
import os
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from collections import defaultdict
import string


load_dotenv()                                  # reads .env into environment
conn_string = os.environ["CONNECTION_STRING"]  # pulls your variable out
conn = psycopg2.connect(conn_string)
cur = conn.cursor()

start_timestamp = 1783382400000
end_timestamp = start_timestamp+86400000
def fetchDayRecords(date):
    cur.execute("SELECT id FROM instruments WHERE symbol = %s", ("BTCUSDT",))
    instrument_id = cur.fetchone()[0]
    start_timestamp = date
    end_time = date + 86400000
    first_trade = requests.get("https://api.binance.com/api/v3/aggTrades",params={"symbol":"BTCUSDT","startTime":start_timestamp,"limit": 1}).json()
    from_id = first_trade[0]['a']
    while True:
        response = requests.get("https://api.binance.com/api/v3/aggTrades", params={"symbol":"BTCUSDT","fromId":from_id,"limit":1000}).json()
        if len(response) == 0:
            break
        batch = [t for t in response if t['T'] <= end_time]
        if batch:
            rows = [(t['a'],instrument_id,t['p'],t['q'],t['T'],t['m']) for t in batch]
            execute_values(cur,"INSERT INTO trades (agg_trade_id, instrument_id, price, quantity, ts, is_buyer_maker) "
                "VALUES %s ON CONFLICT (agg_trade_id) DO NOTHING",
                rows,
                template = "(%s, %s, %s, %s   , to_timestamp(%s / 1000.0), %s)")
            conn.commit()
        
        if response[-1]['T'] > end_time:
            break
        from_id = response[-1]['a'] + 1
        if len(response) < 1000:
            break

def get_profile_grid(start_ts_ms):
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

lst = get_profile_grid(start_timestamp)
profile = defaultdict(list)
for price_bucket, period, trade_count, volume in lst:
    profile[price_bucket].append(period)
letters = string.ascii_uppercase + string.ascii_lowercase

counts = {price: len(periods) for price,periods in profile.items()}
max_tpo = 0
poc = 0

levels = sorted(counts.keys())
range_mid = (levels[-1] + levels[0])/2
for price,tpo in counts.items():
    if tpo > max_tpo:
        max_tpo = tpo
        poc = price
    elif tpo == max_tpo and abs(price - range_mid) < abs(poc - range_mid):
        max_tpo = tpo
        poc = price

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
arr_ib = []
arr_single_tpo = []
for price,periods in profile.items():
    if len(periods) == 1:
        arr_single_tpo.append(price)
    if (1 in periods or 0 in periods):
        arr_ib.append(price)

ib_low = min(arr_ib)
ib_high = max(arr_ib)

is_poor_high = counts[levels[-1]] >= 2
is_poor_low = counts[levels[0]] >= 2

# for price in sorted(profile,reverse=True):
#     row = ""
#     for p in sorted(profile[price]):
#         row+= letters[int(p)]
#     print(f'{price} {row}',end='')
#     if price == poc:
#         print(f'   ---> POC',end='')
#     if price == levels[higher_i-1]:
#         print(f'   ---> VAH',end='')
#     if price == levels[lower_i+1]:
#         print(f'   ---> VAL',end='')
#     if price == ib_low:
#         print(f'   ---> IBL',end='')
#     if price == ib_high:
#         print(f'   ---> IBH',end='')
#     if price in arr_single_tpo:
#         print(f'   ---> Single TPO',end='')
#     print()
# if (is_poor_high):
#     print(f'POOR HIGH: {levels[-1]} ',end='')
# if (is_poor_low):
#     print(f'POOR LOW: {levels[0]}',end='')


def segment_profile(levels,counts,thin_max = 1):
    segmented_profile = []
    thick_run = []
    thin_run = []
    for price in sorted(levels,reverse=True):
        if counts[price] >= 2:
            if len(thin_run) != 0:
                segmented_profile.append(["thin",thin_run])
            thin_run = []
            thick_run.append(int(price))
        elif counts[price] <= thin_max:
            if len(thick_run) != 0:
                segmented_profile.append(["thick",thick_run])
            thick_run = []
            thin_run.append(int(price))
    if len(thin_run) != 0:
        segmented_profile.append(["thin",thin_run])
    if len(thick_run) != 0:
        segmented_profile.append(["thick",thick_run])
    return segmented_profile
        
def detect_double_distribution_split(segments, min_thick_levels=3):
    segmented_profile = segments
    flag = False
    for i in range(len(segmented_profile) - 2):
        a, b, c = segments[i], segments[i+1], segments[i+2]
        if a[0] == 'thick' and len(a[1]) >= min_thick_levels \
            and b[0] == 'thin' \
            and c[0] == 'thick' and len(c[1]) >= min_thick_levels:
                return True
    return False

# # ---- TEST 1: pure tail — one distribution, long single-print tail. MUST be False ----
# tail_counts = {100: 8, 99: 9, 98: 10, 97: 8,           # one fat distribution
#                96: 1, 95: 1, 94: 1, 93: 1, 92: 1, 91: 1,  # long excess tail
#                90: 1, 89: 1, 88: 1, 87: 1, 86: 1, 85: 1}  # of single prints
# tail_levels = sorted(tail_counts.keys())
# assert detect_double_distribution_split(segment_profile(tail_levels, tail_counts, 1)) == False, "TAIL should be False"

# # ---- TEST 2: genuine double distribution — thick, thin gap, thick. MUST be True ----
# dd_counts = {100: 7, 99: 8, 98: 9, 97: 7,     # upper distribution (thick)
#              96: 1, 95: 1, 94: 1,              # single-print gap between them
#              93: 8, 92: 9, 91: 8, 90: 7}       # lower distribution (thick)
# dd_levels = sorted(dd_counts.keys())
# assert detect_double_distribution_split(segment_profile(dd_levels, dd_counts, 1)) == True, "DOUBLE-DIST should be True"

# # ---- TEST 3: real July 7 data. MUST be False (one distribution, no clean split) ----
# assert detect_double_distribution_split(segment_profile(levels, counts, 1)) == False, "JULY 7 should be False"

# print("all three passed")   
# 

def detect_trend(start_timestamp):
    cur.execute('''SELECT
    FLOOR(EXTRACT(EPOCH FROM (ts AT TIME ZONE 'UTC')::time) / 1800) AS period,
    MAX(price) AS period_high,
    MIN(price) AS period_low
    FROM trades
    WHERE ts >= to_timestamp(%s / 1000.0) AND ts < to_timestamp(%s / 1000.0)
    GROUP BY period
    ORDER BY period''',(start_timestamp,start_timestamp+86400000))
    periods = cur.fetchall()
    uptrend_violation = 0
    downtrend_violation = 0
    for i in range(1,len(periods)):
        if periods[i-1][2] > periods[i][2]:
            uptrend_violation += 1
        if periods[i-1][1] < periods[i][1]:
            downtrend_violation += 1

    return (1-(uptrend_violation/(len(periods)-1)),1-(downtrend_violation/(len(periods)-1)))

# detect_trend(start_timestamp)

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
uptrend_confirmation,downtrend_confirmation = detect_trend(start_timestamp)
print(classify_day_type(start_timestamp, ib_high, ib_low, max(levels), min(levels),
                      detect_double_distribution_split(segment_profile(levels, counts, 1)), uptrend_confirmation, downtrend_confirmation))




                    
        
            
    

    