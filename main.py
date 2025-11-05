from fastapi import FastAPI, HTTPException, Query
from database import engine
import pandas as pd
from typing import Optional

app = FastAPI()

@app.get("/")
def home():
    return {"message": "API is running! Go to /docs to test it."}

# --- ENDPOINT 1: Historical Lookup (Updated for 3 boxes) ---
@app.get("/weather/history")
def get_historical_weather(
    year: Optional[int] = Query(None, description="Optional: Specific year (e.g., 2015)"),
    month: Optional[int] = Query(None, ge=1, le=12, description="Required: Month (1-12)"),
    day: Optional[int] = Query(None, ge=1, le=31, description="Optional: Day (1-31)")
):
    # Error Handling: Enforce specifically requested constraints
    if year is not None and month is None:
         raise HTTPException(status_code=400, detail="If you provide a YEAR, you must also provide a MONTH.")
    if month is None:
         raise HTTPException(status_code=400, detail="MONTH is required for this lookup.")

    # Start with a base query that is always true
    query = "SELECT * FROM weather_logs WHERE 1=1"
    
    # Dynamically add filters based on which boxes the user filled in
    if year is not None:
        query += f" AND year = {year}"
    if month is not None:
        query += f" AND month = {month}"
    if day is not None:
        query += f" AND day = {day}"

    try:
        df = pd.read_sql(query, engine)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if df.empty:
        return {"message": "No data found for these filters."}
    
    return df.to_dict(orient="records")

# --- ENDPOINT 2: Monthly Aggregations (Stays the same, it was already correct) ---
@app.get("/weather/summary")
def get_yearly_summary(year: int = Query(..., description="Required: Year for summary (e.g., 2015)")):
    query = f"SELECT month, temperature FROM weather_logs WHERE year = {year}"
    try:
        df = pd.read_sql(query, engine)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if df.empty:
        return {"message": f"No data found for year {year}."}

    # Calculate high, low, and median for all 12 months at once
    summary = df.groupby('month')['temperature'].agg(['max', 'min', 'median']).reset_index()
    summary.rename(columns={'max': 'high_temp', 'min': 'low_temp', 'median': 'median_temp'}, inplace=True)
    
    return summary.to_dict(orient="records")