import requests
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()                                  # reads .env into environment
conn_string = os.environ["CONNECTION_STRING"]  # pulls your variable out
response = requests.get("https://api.binance.com/api/v3/aggTrades",params={"symbol":"BTCUSDT","limit": 1000})
conn = psycopg2.connect(conn_string)
cur = conn.cursor()

