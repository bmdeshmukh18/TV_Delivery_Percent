import pandas as pd
import requests
import io
from datetime import datetime, timedelta
import numpy as np
import time
import os

# 2. Define constants
output_csv_file = 'data/nse_eq_combined_data.csv'
initial_start_date_str = '2019-10-01'
initial_end_date_str = '2026-02-05'
holidays_md = [(1, 26), (8, 15), (10, 2)] # (month, day) for Jan 26, Aug 15, Oct 2

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
            except ValueError: # Handles cases like Feb 29 in non-leap years if day was 29
                pass
    holidays_dt_index = pd.DatetimeIndex(holidays).normalize()

    # Filter out these public holidays from the list of valid dates
    valid_dates = working_days[~working_days.isin(holidays_dt_index)]
    
    return valid_dates

# Helper function to convert columns to numeric, coercing errors
def convert_numeric_columns(df):
    for col in ['DELIV_QTY', 'DELIV_PER']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

# 5. Initialize an empty list to store filtered DataFrames for new data
newly_fetched_data_frames = []
combined_df = pd.DataFrame() # Initialize combined_df

# Define dtypes for problematic columns to read them as strings, avoiding DtypeWarning
column_dtypes_for_read = {col: str for col in ['DELIV_QTY', 'DELIV_PER']}

# 4. Check if output_csv_file exists
if not os.path.exists(output_csv_file):
    # 6a. If output_csv_file DOES NOT exist
    print("Performing initial data fetch...")
    start_date = pd.to_datetime(initial_start_date_str)
    end_date = pd.to_datetime(initial_end_date_str)
    valid_dates_to_fetch = generate_valid_dates(start_date, end_date, holidays_md)
else:
    # 6b. If output_csv_file DOES exist
    print(f"'{output_csv_file}' found. Checking for new data...")
    
    # Read the CSV with problematic columns explicitly as string type, then convert to numeric
    combined_df = pd.read_csv(output_csv_file, parse_dates=['TRADE_DATE'], on_bad_lines='skip', dtype=column_dtypes_for_read)
    combined_df = convert_numeric_columns(combined_df) # Apply conversion

    # Ensure 'TRADE_DATE' is datetime and get the latest date
    if 'TRADE_DATE' in combined_df.columns and not combined_df['TRADE_DATE'].empty:
        combined_df['TRADE_DATE'] = pd.to_datetime(combined_df['TRADE_DATE'])
        latest_existing_date = combined_df['TRADE_DATE'].max()
        print(f"Latest existing data is for: {latest_existing_date.strftime('%Y-%m-%d')}")
        new_fetch_start_date = latest_existing_date + timedelta(days=1)
    else:
        # If for some reason TRADE_DATE is missing or empty, re-fetch all initial data
        print("'TRADE_DATE' column not found or is empty in existing CSV. Performing initial fetch.")
        new_fetch_start_date = pd.to_datetime(initial_start_date_str)
        combined_df = pd.DataFrame() # Reset combined_df if it's invalid or incomplete

    current_local_time = pd.Timestamp.now()
    today_date_normalized = current_local_time.normalize()

    # 6b.vi. If current hour < 18 (before 6 PM) and new_fetch_start_date <= today's date
    if current_local_time.hour < 18 and new_fetch_start_date <= today_date_normalized:
        print("Today's Bhavcopy data is usually available after 6 PM. Skipping data fetch for today.")
        valid_dates_to_fetch = pd.DatetimeIndex([]) # No dates to fetch
    elif new_fetch_start_date > today_date_normalized:
        print("No new dates to fetch yet. Existing data is up-to-date or future dates.")
        valid_dates_to_fetch = pd.DatetimeIndex([])
    else:
        # 6b.vii. Otherwise (current time is 6 PM or later, or new_fetch_start_date is in the past)
        print(f"Fetching data from {new_fetch_start_date.strftime('%Y-%m-%d')} to {today_date_normalized.strftime('%Y-%m-%d')}")
        fetch_end_date = today_date_normalized
        valid_dates_to_fetch = generate_valid_dates(new_fetch_start_date, fetch_end_date, holidays_md)

# 7. Loop through each date_obj in the determined valid_dates_to_fetch
for date_obj in valid_dates_to_fetch:
    # a. Format the date_obj into a string in DDMMYYYY format
    formatted_date = date_obj.strftime('%d%m%Y')

    # b. Construct the URL for the Bhavcopy data
    url = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{formatted_date}.csv"

    try:
        # c. Use a try-except block to handle potential requests.exceptions.RequestException
        response = requests.get(url, timeout=10) # Add a timeout for robustness
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)

        # d. Read the content into a Pandas DataFrame
        # Check if the response content is empty or looks like an error message
        if not response.text.strip() or "Error occured" in response.text or "No Data" in response.text:
            print(f"No data or error message found for {date_obj.strftime('%Y-%m-%d')}. Skipping.")
            continue

        # Use StringIO to treat the string content as a file, read problematic columns as strings, and handle bad lines
        df_daily = pd.read_csv(io.StringIO(response.text), on_bad_lines='skip', dtype=column_dtypes_for_read)
        df_daily = convert_numeric_columns(df_daily) # Apply conversion
        
        # If DataFrame is empty after reading, skip
        if df_daily.empty:
            print(f"DataFrame is empty for {date_obj.strftime('%Y-%m-%d')}. Skipping.")
            continue

        # e. Strip any leading or trailing whitespace from the DataFrame's column names.
        df_daily.columns = df_daily.columns.str.strip()

        # f. Filter this DataFrame to include only rows where the 'SERIES' column value, after stripping whitespace, is exactly 'EQ'.
        # Ensure 'SERIES' column exists before filtering
        if 'SERIES' in df_daily.columns:
            df_filtered_daily = df_daily[df_daily['SERIES'].str.strip() == 'EQ']
        else:
            print(f"'SERIES' column not found in data for {date_obj.strftime('%Y-%m-%d')}. Skipping.")
            continue

        # g. If the filtered DataFrame is not empty, append it to the newly_fetched_data_frames list.
        if not df_filtered_daily.empty:
            # Add a 'TRADE_DATE' column to keep track of the date for each record
            # Use .loc to avoid SettingWithCopyWarning
            df_filtered_daily = df_filtered_daily.copy()
            df_filtered_daily.loc[:, 'TRADE_DATE'] = date_obj.strftime('%Y-%m-%d')
            newly_fetched_data_frames.append(df_filtered_daily)
            print(f"Successfully processed and added data for {date_obj.strftime('%Y-%m-%d')}. Filtered rows: {len(df_filtered_daily)}")

    except requests.exceptions.RequestException as e:
        # c. Print an informative message if an error occurs
        print(f"Error downloading or reading data for {date_obj.strftime('%Y-%m-%d')}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred for {date_obj.strftime('%Y-%m-%d')}: {e}")

    # h. Add a small time delay to avoid overwhelming the server
    time.sleep(0.2)

# 8. After the loop, concatenate all DataFrames in newly_fetched_data_frames into a single DataFrame
if newly_fetched_data_frames:
    new_data_df = pd.concat(newly_fetched_data_frames, ignore_index=True)
    print("Newly fetched data successfully combined.")
else:
    new_data_df = pd.DataFrame() # Create an empty DataFrame if no new data was processed
    print("No new data was successfully processed to combine.")

# 9. Combine combined_df (from the existing file, if applicable) and new_data_df
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

# 10. Save the final combined_df to output_csv_file
if not combined_df.empty:
    combined_df.to_csv(output_csv_file, index=False)
    print(f"\nFinal combined data saved successfully to '{output_csv_file}'")
else:
    print("\nNo data to save to CSV.")

# 11. Print the shape of the final combined_df
print(f"Shape of the final combined_df: {combined_df.shape}")

# 12. Display the head of the final combined_df
if not combined_df.empty:
    print("\nFirst 5 rows of the final combined_df:")
    display(combined_df.head())
else:
    print("Final combined DataFrame is empty, no data to display.")
