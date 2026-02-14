import pandas as pd
import requests
import io
from datetime import datetime, timedelta
import numpy as np
import time
import os
import json

# 2. Define constants
chart_dir = 'StockData'
symbol_info_json_path = os.path.join(chart_dir, '0_symbolInfo.json')
initial_start_date_str = '2019-10-01'
holidays_md = [(1, 26), (8, 15), (10, 2)]  # (month, day)

# 3. Helper functions
def generate_valid_dates(start, end, holidays_month_day):
    all_dates = pd.date_range(start=start, end=end, freq='D')
    working_days = all_dates[all_dates.dayofweek < 5]
    holidays = []
    for year in range(start.year, end.year + 1):
        for month, day in holidays_month_day:
            try:
                holidays.append(pd.to_datetime(f'{year}-{month:02d}-{day:02d}'))
            except ValueError:
                pass
    holidays_dt_index = pd.DatetimeIndex(holidays).normalize()
    return working_days[~working_days.isin(holidays_dt_index)]

def convert_numeric_columns(df):
    for col in ['DELIV_QTY','DELIV_PER','PREV_CLOSE','OPEN_PRICE','HIGH_PRICE','LOW_PRICE','CLOSE_PRICE']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

# 5. Initialize
newly_fetched_data_frames = []
combined_df = pd.DataFrame()
column_dtypes_for_read = {col: str for col in ['DELIV_QTY','DELIV_PER','PREV_CLOSE','OPEN_PRICE','HIGH_PRICE','LOW_PRICE','CLOSE_PRICE']}
today_date_normalized = pd.Timestamp.now().normalize()

# Always perform initial fetch (no combined CSV anymore)
start_date = pd.to_datetime(initial_start_date_str)
valid_dates_to_fetch = generate_valid_dates(start_date, today_date_normalized, holidays_md)

# 7. Fetch loop
for date_obj in valid_dates_to_fetch:
    formatted_date = date_obj.strftime('%d%m%Y')
    url = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{formatted_date}.csv"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        if not response.text.strip() or "Error occured" in response.text or "No Data" in response.text:
            print(f"No data for {date_obj.strftime('%Y-%m-%d')}. Skipping.")
            continue
        df_daily = pd.read_csv(io.StringIO(response.text), on_bad_lines='skip', dtype=column_dtypes_for_read)
        df_daily = convert_numeric_columns(df_daily)
        if df_daily.empty: continue
        df_daily.columns = df_daily.columns.str.strip()
        if 'SERIES' not in df_daily.columns: continue
        df_filtered_daily = df_daily[df_daily['SERIES'].str.strip() == 'EQ']
        if df_filtered_daily.empty: continue
        df_filtered_daily = df_filtered_daily.copy()
        df_filtered_daily.loc[:, 'TRADE_DATE'] = date_obj.strftime('%Y-%m-%d')
        required_columns = ['SYMBOL','TRADE_DATE','DELIV_PER','PREV_CLOSE','OPEN_PRICE','HIGH_PRICE','LOW_PRICE','CLOSE_PRICE']
        if all(col in df_filtered_daily.columns for col in required_columns):
            df_filtered_daily = df_filtered_daily[required_columns]
            df_filtered_daily = convert_numeric_columns(df_filtered_daily)
            df_filtered_daily['Change_Percentage'] = (((df_filtered_daily['CLOSE_PRICE'] - df_filtered_daily['PREV_CLOSE']) / df_filtered_daily['PREV_CLOSE']) * 100).round(2)
            newly_fetched_data_frames.append(df_filtered_daily)
            print(f"Processed {date_obj.strftime('%Y-%m-%d')}, rows: {len(df_filtered_daily)}")
    except Exception as e:
        print(f"Error for {date_obj.strftime('%Y-%m-%d')}: {e}")
    time.sleep(0.2)

# 8. Combine
if newly_fetched_data_frames:
    combined_df = pd.concat(newly_fetched_data_frames, ignore_index=True)
    print("Combined all newly fetched data.")
else:
    print("No data fetched.")
    combined_df = pd.DataFrame()

# --- New Part: Directly generate per-symbol CSVs ---
if not combined_df.empty:
    # Ensure Chart directory exists
    os.makedirs(chart_dir, exist_ok=True)

    # Load last scanned date if exists
    last_scanned_date = None
    if os.path.exists(symbol_info_json_path):
        try:
            with open(symbol_info_json_path,'r') as f:
                symbol_info_data = json.load(f)
                if 'LastDateScanned' in symbol_info_data:
                    last_scanned_date = pd.to_datetime(symbol_info_data['LastDateScanned'])
                    print(f"LastDateScanned: {last_scanned_date.strftime('%Y-%m-%d')}")
        except Exception:
            print("Could not decode existing symbolInfo.json, starting fresh.")

    # Filter incremental
    if last_scanned_date is not None:
        df_filtered = combined_df[pd.to_datetime(combined_df['TRADE_DATE']) > last_scanned_date].copy()
    else:
        df_filtered = combined_df.copy()

    if df_filtered.empty:
        print("No new data to process for individual CSVs.")
    else:
        df_filtered['Date'] = pd.to_datetime(df_filtered['TRADE_DATE']).dt.strftime('%d%m%Y')
        df_filtered['DelPerc'] = df_filtered['DELIV_PER']
        df_transformed = df_filtered[['SYMBOL','Date','DelPerc','PREV_CLOSE','OPEN_PRICE','HIGH_PRICE','LOW_PRICE','CLOSE_PRICE','Change_Percentage']]
        grouped = df_transformed.groupby('SYMBOL')

        for symbol, symbol_df in grouped:
            output_file_path = os.path.join(chart_dir, f'{symbol}.csv')
            if os.path.exists(output_file_path):
                existing_df = pd.read_csv(output_file_path, dtype={'Date': str})
                combined_symbol_df = pd.concat([existing_df, symbol_df.drop(columns=['SYMBOL'])]) \
                                        .drop_duplicates(subset=['Date'], keep='last')
                combined_symbol_df['Date'] = pd.to_datetime(combined_symbol_df['Date'], format='%d%m%Y')
                combined_symbol_df = combined_symbol_df.sort_values(by='Date').reset_index(drop=True)
                combined_symbol_df['Date'] = combined_symbol_df['Date'].dt.strftime('%d%m%Y')
                combined_symbol_df.to_csv(output_file_path, index=False)
                print(f"Updated {output_file_path}")
            else:
                symbol_df.drop(columns=['SYMBOL']).to_csv(output_file_path, index=False)
                print(f"Created {output_file_path}")

        # Update symbolInfo.json
        unique_symbols = combined_df['SYMBOL'].unique().tolist()
        latest_trade_date = pd.to_datetime(combined_df['TRADE_DATE']).max()
        symbol_info_data_final = {
            "symbol": unique_symbols,
            "pricescale": 2,
            "LastDateScanned": latest_trade_date.strftime('%Y-%m-%d')
        }
        with open(symbol_info_json_path,'w') as f:
            json.dump(symbol_info_data_final,f,indent=4)
        print(f"Updated {symbol_info_json_path} with {len(unique_symbols)} symbols.")
else:
    print("No combined data available to generate per-symbol CSVs.")
