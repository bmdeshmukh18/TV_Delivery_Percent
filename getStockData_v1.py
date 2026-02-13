import os
import json
import pandas as pd
import requests
import io
from datetime import datetime, timedelta

# -------------------------------
# CONFIGURATION
# -------------------------------
OUTPUT_DIR = "StockData"
SYMBOL_INFO_FILE = os.path.join(OUTPUT_DIR, "0_symbolInfo.json")
INITIAL_START_DATE = "2019-10-01"

# Indian holidays (fixed dates)
HOLIDAYS = [(1, 26), (8, 15), (10, 2)]


# -------------------------------
# UTILITY FUNCTIONS
# -------------------------------

def generate_valid_dates(start_date, end_date, holidays):
    """Generate working days excluding weekends and holidays."""
    all_days = pd.date_range(start=start_date, end=end_date, freq="D")
    working_days = all_days[all_days.dayofweek < 5]

    holiday_dates = []
    for year in range(start_date.year, end_date.year + 1):
        for month, day in holidays:
            try:
                holiday_dates.append(pd.Timestamp(f"{year}-{month:02d}-{day:02d}"))
            except ValueError:
                pass

    holiday_index = pd.DatetimeIndex(holiday_dates).normalize()
    return working_days[~working_days.isin(holiday_index)]


def load_last_scan_date():
    """Read LastDateScanned from JSON."""
    if not os.path.exists(SYMBOL_INFO_FILE):
        return None

    try:
        with open(SYMBOL_INFO_FILE, "r") as f:
            data = json.load(f)
            return pd.to_datetime(data.get("LastDateScanned"))
    except Exception:
        return None


def save_symbol_info(symbols, last_date):
    """Write JSON file with symbol list and last scanned date."""
    data = {
        "symbols": sorted(list(symbols)),
        "pricescale": 2,
        "LastDateScanned": last_date.strftime("%Y-%m-%d")
    }
    with open(SYMBOL_INFO_FILE, "w") as f:
        json.dump(data, f, indent=4)


def fetch_nse_csv(date_obj):
    """Download NSE bhavcopy file."""
    date_str = date_obj.strftime("%d%m%Y")
    url = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        if "Error" in response.text or "No Data" in response.text or not response.text.strip():
            return None

        return pd.read_csv(io.StringIO(response.text), on_bad_lines="skip")

    except Exception:
        return None


# -------------------------------
# MAIN PROCESS
# -------------------------------

def main():
    # Create output folder
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Determine start date
    last_scanned_date = load_last_scan_date()
    today = pd.Timestamp.now().normalize()

    if last_scanned_date is None:
        start_date = pd.to_datetime(INITIAL_START_DATE)
        print("Performing full initial fetch...")
    else:
        start_date = last_scanned_date + timedelta(days=1)
        print(f"Fetching from {start_date.date()} onward...")

    if start_date > today:
        print("Nothing to fetch — data already up‑to‑date.")
        return

    valid_dates = generate_valid_dates(start_date, today, HOLIDAYS)
    all_symbols = set()

    for date_obj in valid_dates:
        print(f"Processing {date_obj.date()}...")

        df = fetch_nse_csv(date_obj)
        if df is None or df.empty:
            print(f"Skipping {date_obj.date()} — no data.")
            continue

        df.columns = df.columns.str.strip()

        # Ensure required columns exist
        required_cols = [
            "SERIES", "SYMBOL", "DATE1",
            "OPEN_PRICE", "HIGH_PRICE", "LOW_PRICE",
            "CLOSE_PRICE", "PREV_CLOSE", "DELIV_PER"
        ]

        if not all(col in df.columns for col in required_cols):
            print(f"Missing required columns on {date_obj.date()} — skipped.")
            continue

        # Filter EQ only
        df = df[df["SERIES"].str.strip() == "EQ"].copy()
        if df.empty:
            continue

        # Prepare cleaned data
        df["TRADE_DATE"] = pd.to_datetime(df["DATE1"], errors="coerce")
        df = df.dropna(subset=["TRADE_DATE"])

        df["TRADE_DATE"] = df["TRADE_DATE"].dt.strftime("%d%m%Y")

        # Calculate CHANGE_PERCENT
        df["CHANGE_PERCENT"] = (
            (df["CLOSE_PRICE"] - df["PREV_CLOSE"]) / df["PREV_CLOSE"] * 100
        )

        # Keep only cleaned columns
        df = df.rename(columns={
            "OPEN_PRICE": "OPEN",
            "HIGH_PRICE": "HIGH",
            "LOW_PRICE": "LOW",
            "CLOSE_PRICE": "CLOSE",
            "DELIV_PER": "DELIVERY_PERCENT"
        })

        df = df[[
            "SYMBOL", "TRADE_DATE", "OPEN", "HIGH", "LOW",
            "CLOSE", "DELIVERY_PERCENT", "CHANGE_PERCENT"
        ]]

        # Store per‑symbol CSV files
        for symbol, sdf in df.groupby("SYMBOL"):
            symbol_file = os.path.join(OUTPUT_DIR, f"{symbol}.csv")
            all_symbols.add(symbol)

            if os.path.exists(symbol_file):
                existing = pd.read_csv(symbol_file, dtype=str)
                combined = pd.concat([existing, sdf], ignore_index=True)
                combined = combined.drop_duplicates(subset=["TRADE_DATE"], keep="last")
            else:
                combined = sdf.copy()

            # Preserve date format DDMMYYYY
            combined["TRADE_DATE"] = combined["TRADE_DATE"].astype(str)
            combined = combined.sort_values(by="TRADE_DATE")

            combined.to_csv(symbol_file, index=False)

        print(f"Saved {len(df)} rows for {date_obj.date()}.")

    # Update JSON metadata
    latest_date = valid_dates[-1] if len(valid_dates) else last_scanned_date
    if latest_date is not None:
        save_symbol_info(all_symbols, latest_date)

    print("\nProcessing completed successfully.")


if __name__ == "__main__":
    main()
