from fastapi import FastAPI,HTTPException
from ingest import compute_structures, generate_report  # import your engine

app = FastAPI()

@app.get("/profile/{start_ts}")
def get_profile(start_ts: int):
    try:
        return compute_structures(start_ts)   # FastAPI auto-converts the dict to JSON
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.get("/report/{start_ts}")
def get_report(start_ts: int):
    try:
        return {"report": generate_report(compute_structures(start_ts))}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))