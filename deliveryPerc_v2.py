import pandas as pd
import requests
import io
from datetime import datetime, timedelta
import numpy as np
import time
import os

# 2. Define constants
output_csv_file = 'data/nse_eq_combined_deliveryPerc.csv'
initial_start_date_str = '2021-01-01'
holidays_md = [(1, 26), (8, 15), (10, 2)]  # (month, day) for Jan 26, Aug 15, Oct 2

# 3. Define a helper function to generate valid dates
def generate_valid_dates(start, end, holidays_month_day):
    all_dates = pd.date_range(start=start, end=end, freq='D')

    # Filter out weekends (Saturdays and Sundays)
    working_days = all_dates[all_dates.dayofweek < 5]

    # Generate holiday dates for each year in the range
    holidays = []
    for year in range(start.year, end.year + 1):
        for month, day in holidays_month_day:
            try:
                holidays.append(pd.to_datetime(f'{year}-{month:02d}-{day:02d}'))
            except ValueError:
                pass
    holidays_dt_index = pd.DatetimeIndex(holidays).normalize()

    # Filter out these public holidays from the list of valid dates
    valid_dates = working_days[~working_days.isin(holidays_dt_index)]

    return valid_dates

# Helper function to convert columns to numeric, coercing errors
def convert_numeric_columns(df):
    for col in ['DELIV_QTY', 'DELIV_PER', 'PREV_CLOSE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

# 5. Initialize an empty list to store filtered DataFrames for new data
newly_fetched_data_frames = []
combined_df = pd.DataFrame()

# Define dtypes for problematic columns to read them as strings
column_dtypes_for_read = {col: str for col in ['DELIV_QTY', 'DELIV_PER', 'PREV_CLOSE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE']}

# Current date (normalized to midnight)
today_date_normalized = pd.Timestamp.now().normalize()

# 4. Check if output_csv_file exists
if not os.path.exists(output_csv_file):
    print("Performing initial data fetch...")
    start_date = pd.to_datetime(initial_start_date_str)
    valid_dates_to_fetch = generate_valid_dates(start_date, today_date_normalized, holidays_md)
else:
    print(f"'{output_csv_file}' found. Checking for new data...")

    # When reading, let pandas infer date format more broadly initially for TRADE_DATE
    combined_df = pd.read_csv(output_csv_file, parse_dates=['TRADE_DATE'], on_bad_lines='skip', dtype=column_dtypes_for_read)
    combined_df = convert_numeric_columns(combined_df)

    if 'TRADE_DATE' in combined_df.columns and not combined_df['TRADE_DATE'].empty:
        # Convert to datetime, inferring format, and then normalize to keep only date
        combined_df['TRADE_DATE'] = pd.to_datetime(combined_df['TRADE_DATE'], errors='coerce').dt.normalize()
        latest_existing_date = combined_df['TRADE_DATE'].max().normalize() # Normalize for accurate comparison
        print(f"Latest existing data is for: {latest_existing_date.strftime('%Y-%m-%d')}")

        if latest_existing_date == today_date_normalized:
            print(f"Data for the current date ({today_date_normalized.strftime('%Y-%m-%d')}) already exists. Script is up-to-date. No new data will be fetched.")
            valid_dates_to_fetch = pd.DatetimeIndex([]) # No dates to fetch, effectively exits new data fetching.
        elif latest_existing_date > today_date_normalized:
            # This handles cases where the existing data is somehow in the future.
            print(f"Existing data ({latest_existing_date.strftime('%Y-%m-%d')}) is newer than or equal to current date ({today_date_normalized.strftime('%Y-%m-%d')}). No new data will be fetched.")
            valid_dates_to_fetch = pd.DatetimeIndex([])
        else: # latest_existing_date < today_date_normalized
            new_fetch_start_date = latest_existing_date + timedelta(days=1)
            print(f"Fetching data from {new_fetch_start_date.strftime('%Y-%m-%d')} to {today_date_normalized.strftime('%Y-%m-%d')}")
            valid_dates_to_fetch = generate_valid_dates(new_fetch_start_date, today_date_normalized, holidays_md)
    else:
        # This branch is for when 'TRADE_DATE' column is missing or empty, implying a corrupted or new file
        print("'TRADE_DATE' column not found or is empty. Performing initial fetch.")
        new_fetch_start_date = pd.to_datetime(initial_start_date_str)
        combined_df = pd.DataFrame()
        valid_dates_to_fetch = generate_valid_dates(new_fetch_start_date, today_date_normalized, holidays_md)

# 7. Loop through each date_obj in the determined valid_dates_to_fetch
for date_obj in valid_dates_to_fetch:
    formatted_date = date_obj.strftime('%d%m%Y')
    url = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{formatted_date}.csv"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        if not response.text.strip() or "Error occured" in response.text or "No Data" in response.text:
            print(f"No data or error message found for {date_obj.strftime('%Y-%m-%d')}. Skipping.")
            continue

        df_daily = pd.read_csv(io.StringIO(response.text), on_bad_lines='skip', dtype=column_dtypes_for_read)
        df_daily = convert_numeric_columns(df_daily)

        if df_daily.empty:
            print(f"DataFrame is empty for {date_obj.strftime('%Y-%m-%d')}. Skipping.")
            continue

        df_daily.columns = df_daily.columns.str.strip()

        if 'SERIES' in df_daily.columns:
            df_filtered_daily = df_daily[df_daily['SERIES'].str.strip() == 'EQ']
        else:
            print(f"'SERIES' column not found for {date_obj.strftime('%Y-%m-%d')}. Skipping.")
            continue

        if not df_filtered_daily.empty:
            df_filtered_daily = df_filtered_daily.copy()
            df_filtered_daily.loc[:, 'TRADE_DATE'] = date_obj.strftime('%Y-%m-%d')
            # Select only the required columns: SYMBOL, TRADE_DATE, DELIV_PER and the new price columns
            required_columns = ['SYMBOL', 'TRADE_DATE', 'DELIV_PER',
                                'PREV_CLOSE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE']
            # Ensure all required columns exist before selecting to avoid KeyError
            existing_required_columns = [col for col in required_columns if col in df_filtered_daily.columns]
            if len(existing_required_columns) == len(required_columns):
                df_filtered_daily = df_filtered_daily[existing_required_columns]
                # Ensure numeric conversion again for safety
                df_filtered_daily = convert_numeric_columns(df_filtered_daily)
                # Add Change_Percentage column rounded to 2 decimals
                df_filtered_daily['Change_Percentage'] = (((df_filtered_daily['CLOSE_PRICE'] - df_filtered_daily['PREV_CLOSE']) / df_filtered_daily['PREV_CLOSE']) * 100).round(2)
                newly_fetched_data_frames.append(df_filtered_daily)
                print(f"Successfully processed and added data for {date_obj.strftime('%Y-%m-%d')}. Rows: {len(df_filtered_daily)}")
            else:
                print(f"Missing one or more required columns (SYMBOL, TRADE_DATE, DELIV_PER, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE) for {date_obj.strftime('%Y-%m-%d')}. Skipping.")

    except requests.exceptions.RequestException as e:
        print(f"Error downloading data for {date_obj.strftime('%Y-%m-%d')}: {e}")
    except Exception as e:
        print(f"Unexpected error for {date_obj.strftime('%Y-%m-%d')}: {e}")

    time.sleep(0.2)

# 8. Combine newly fetched data
if newly_fetched_data_frames:
    new_data_df = pd.concat(newly_fetched_data_frames, ignore_index=True)
    print("Newly fetched data successfully combined.")
else:
    new_data_df = pd.DataFrame()
    print("No new data was successfully processed to combine.")

# 9. Merge with existing data
# When reading existing combined_df, ensure only required columns are loaded if it exists
if os.path.exists(output_csv_file):
    # Re-read with parse_dates to ensure proper type handling, and then normalize
    temp_combined_df = pd.read_csv(output_csv_file, parse_dates=['TRADE_DATE'], on_bad_lines='skip', dtype=column_dtypes_for_read)
    temp_combined_df = convert_numeric_columns(temp_combined_df)
    # Ensure TRADE_DATE is normalized after reading from CSV
    if 'TRADE_DATE' in temp_combined_df.columns and not temp_combined_df['TRADE_DATE'].empty:
        temp_combined_df['TRADE_DATE'] = pd.to_datetime(temp_combined_df['TRADE_DATE'], errors='coerce').dt.normalize()

    # Filter to keep only the desired columns from existing data too
    existing_required_columns = ['SYMBOL', 'TRADE_DATE', 'DELIV_PER',
                                 'PREV_CLOSE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE', 'Change_Percentage']
    if all(col in temp_combined_df.columns for col in existing_required_columns):
        combined_df = temp_combined_df[existing_required_columns]
    else:
        # If Change_Percentage is missing but price columns exist, compute it
        if all(col in temp_combined_df.columns for col in ['SYMBOL', 'TRADE_DATE', 'DELIV_PER', 'PREV_CLOSE', 'CLOSE_PRICE']):
            temp_combined_df = temp_combined_df.copy()
            temp_combined_df['Change_Percentage'] = (((pd.to_numeric(temp_combined_df['CLOSE_PRICE'], errors='coerce') - pd.to_numeric(temp_combined_df['PREV_CLOSE'], errors='coerce')) / pd.to_numeric(temp_combined_df['PREV_CLOSE'], errors='coerce')) * 100).round(2)
            # Ensure all expected columns exist; if some price columns are missing, add them as NaN
            for col in ['PREV_CLOSE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE', 'Change_Percentage']:
                if col not in temp_combined_df.columns:
                    temp_combined_df[col] = np.nan
            combined_df = temp_combined_df[['SYMBOL', 'TRADE_DATE', 'DELIV_PER', 'PREV_CLOSE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE', 'Change_Percentage']]
        else:
            print("Warning: Existing CSV does not contain all required columns. Starting fresh or merging only available columns.")
            # Start with empty DataFrame with the full set of columns
            combined_df = pd.DataFrame(columns=['SYMBOL', 'TRADE_DATE', 'DELIV_PER',
                                                'PREV_CLOSE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE', 'Change_Percentage'])

if not combined_df.empty and not new_data_df.empty:
    combined_df = pd.concat([combined_df, new_data_df], ignore_index=True)
    print("Existing data combined with newly fetched data.")
elif combined_df.empty and not new_data_df.empty:
    combined_df = new_data_df
    print("Initialized combined_df with newly fetched data.")
elif not combined_df.empty and new_data_df.empty:
    print("No new data fetched. Using existing combined_df.")
else:
    print("No data to process or combine.")

# Ensure numeric types for price columns and compute Change_Percentage if missing or NaN
if not combined_df.empty:
    combined_df = convert_numeric_columns(combined_df)
    # If Change_Percentage column missing, create it (rounded to 2 decimals)
    if 'Change_Percentage' not in combined_df.columns:
        combined_df['Change_Percentage'] = (((combined_df['CLOSE_PRICE'] - combined_df['PREV_CLOSE']) / combined_df['PREV_CLOSE']) * 100).round(2)
    else:
        # Recompute where NaN and round to 2 decimals
        mask = combined_df['Change_Percentage'].isna()
        if mask.any():
            combined_df.loc[mask, 'Change_Percentage'] = (((combined_df.loc[mask, 'CLOSE_PRICE'] - combined_df.loc[mask, 'PREV_CLOSE']) / combined_df.loc[mask, 'PREV_CLOSE']) * 100).round(2)

# 10. Save to CSV
# Create the 'data' directory if it doesn't exist
os.makedirs(os.path.dirname(output_csv_file), exist_ok=True)

# Ensure final column order
final_columns = ['SYMBOL', 'TRADE_DATE', 'DELIV_PER', 'PREV_CLOSE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE', 'Change_Percentage']

if not combined_df.empty:
    # Add any missing final columns as NaN to maintain consistent schema
    for col in final_columns:
        if col not in combined_df.columns:
            combined_df[col] = np.nan
    combined_df = combined_df[final_columns]
    combined_df.to_csv(output_csv_file, index=False)
    print(f"\nFinal combined data saved successfully to '{output_csv_file}'")
else:
    print("\nNo data to save to CSV.")

# 11. Print shape
print(f"Shape of the final combined_df: {combined_df.shape}")

## 12. Display head
#if not combined_df.empty:
#    print("\nFirst 5 rows of the final combined_df:")
#    display(combined_df.head())
#else:
#    print("Final combined DataFrame is empty, no data to display.")
