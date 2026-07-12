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

for price in sorted(profile,reverse=True):
    row = ""
    for p in sorted(profile[price]):
        row+= letters[int(p)]
    print(f'{price} {row}',end='')
    if price == poc:
        print(f'   ---> POC',end='')
    if price == levels[higher_i-1]:
        print(f'   ---> VAH',end='')
    if price == levels[lower_i+1]:
        print(f'   ---> VAL',end='')
    if price == ib_low:
        print(f'   ---> IBL',end='')
    if price == ib_high:
        print(f'   ---> IBH',end='')
    if price in arr_single_tpo:
        print(f'   ---> Single TPO',end='')
    print()
if (is_poor_high):
    print(f'POOR HIGH: {levels[-1]} ',end='')
if (is_poor_low):
    print(f'POOR LOW: {levels[0]}',end='')