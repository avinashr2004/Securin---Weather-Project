import pandas as pd
import os
import numpy as np
from database import engine

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Make sure this matches your actual CSV filename exactly
CSV_PATH = os.path.join(BASE_DIR, "testset.csv")

def load_data():
    if not os.path.exists(CSV_PATH):
        print(f"ERROR: {CSV_PATH} not found.")
        return

    print(f"Reading CSV from: {CSV_PATH}")
    try:
        df = pd.read_csv(CSV_PATH, parse_dates=['datetime_utc'])
    except ValueError as e:
        print(f"Error: Could not find 'datetime_utc'. Error details: {e}")
        return

    print(f"Initial row count: {len(df)}")

    column_mapping = {
        'datetime_utc': 'date',
        ' _tempm': 'temperature',
        ' _hum': 'humidity',
        ' _pressurem': 'pressure',
        ' _conds': 'condition'
    }
    
    # Verify columns exist before renaming to avoid later errors
    missing_cols = [col for col in column_mapping.keys() if col not in df.columns]
    if missing_cols:
        print(f"CRITICAL ERROR: Missing columns: {missing_cols}")
        print("Detected columns:", list(df.columns))
        return

    print("Renaming columns...")
    df = df.rename(columns=column_mapping)
    df = df[['date', 'temperature', 'humidity', 'pressure', 'condition']]

    print("Cleaning data...")
    # 1. Drop rows with no date (cannot use them at all)
    df = df.dropna(subset=['date'])

    # 2. Ensure numeric columns are actually numbers (forces text to NaN)
    for col in ['temperature', 'humidity', 'pressure']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 3. Handle Negative Pressure: Treat as missing (NaN)
    # This allows ffill/bfill to replace them with realistic values later
    df.loc[df['pressure'] < 0, 'pressure'] = np.nan

    # 4. Fill all missing values (NaNs) in ALL columns
    # ffill() tries to use the previous row's value first
    # bfill() is a backup in case the very first row is missing data
    df = df.ffill().bfill()

    print("Calculating structure...")
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day

    print("Saving to database...")
    df.to_sql("weather_logs", engine, if_exists='replace', index=False)
    print("DONE! Database is ready with cleaned data.")

if __name__ == "__main__":
    load_data()


# import pandas as pd
# import os
# from database import engine

# # Get the exact location of THIS file
# BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# # Force it to look for CSV in the same folder
# CSV_PATH = os.path.join(BASE_DIR, "testset.csv")

# def load_data():
#     print(f"Looking for CSV file at: {CSV_PATH}")

#     if not os.path.exists(CSV_PATH):
#         print("\nERROR: File NOT found!")
#         print("Please make sure 'weather_data.csv' is inside the 'weather_project' folder.")
#         return

#     print("Found it! Reading data...")
#     df = pd.read_csv(CSV_PATH, parse_dates=['date'])

#     # Pre-calculate columns for speed
#     df['year'] = df['date'].dt.year
#     df['month'] = df['date'].dt.month
#     df['day'] = df['date'].dt.day

#     print("Saving to database...")
#     df.to_sql("weather_logs", engine, if_exists='replace', index=False)
#     print("DONE! Database is ready.")

# if __name__ == "__main__":
#     load_data()