from fastapi import FastAPI
from ingest import compute_structures, generate_report  # import your engine

app = FastAPI()

@app.get("/profile/{start_ts}")
def get_profile(start_ts: int):
    return compute_structures(start_ts)   # FastAPI auto-converts the dict to JSON

@app.get("/report/{start_ts}")
def get_report(start_ts: int):
    return {"report": generate_report(compute_structures(start_ts))}