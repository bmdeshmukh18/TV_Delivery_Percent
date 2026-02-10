import pandas as pd
import os
import json

# 1. Define constants and file paths
output_csv_file_name = 'data/nse_eq_combined_deliveryPerc.csv'
chart_dir = 'Chart'
symbol_info_json_path = os.path.join(chart_dir, '0_symbolInfo.json') # Changed to 0_symbolInfo.json

# 2. Load the nse_eq_combined_deliveryPerc.csv file into a pandas DataFrame
df = pd.read_csv(output_csv_file_name, parse_dates=['TRADE_DATE'], dtype={'DELIV_PER': str})

# Ensure 'DELIV_PER' is numeric, handling errors and filling NaN with 0.0
df['DELIV_PER'] = pd.to_numeric(df['DELIV_PER'], errors='coerce')
df['DELIV_PER'] = df['DELIV_PER'].fillna(0.0)

# Ensure 'TRADE_DATE' is in datetime format
df['TRADE_DATE'] = pd.to_datetime(df['TRADE_DATE'], errors='coerce')
df = df.dropna(subset=['TRADE_DATE']) # Drop rows where TRADE_DATE couldn't be parsed

# 3. Create the 'Chart' directory if it does not already exist
os.makedirs(chart_dir, exist_ok=True)
print(f"Ensured '{chart_dir}' directory exists.")

# 4. Determine Last Scanned Date for Incremental Processing
last_scanned_date = None
if os.path.exists(symbol_info_json_path):
    with open(symbol_info_json_path, 'r') as f:
        try:
            symbol_info_data = json.load(f)
            if 'LastDateScanned' in symbol_info_data:
                last_scanned_date = pd.to_datetime(symbol_info_data['LastDateScanned'])
                print(f"Found LastDateScanned: {last_scanned_date.strftime('%Y-%m-%d')}")
        except json.JSONDecodeError:
            print(f"Warning: Could not decode existing '{symbol_info_json_path}'. Starting fresh.")
        except KeyError:
            print(f"Warning: 'LastDateScanned' not found in existing '{symbol_info_json_path}'. Starting fresh.")
else:
    print(f"'{symbol_info_json_path}' not found. Starting fresh (no previous scan date).")

# Keep a copy of the original dataframe for the final symbol_info.json
original_df_for_json = df.copy()

# 5. Filter Data for Incremental Updates
if last_scanned_date is not None:
    df_filtered = df[df['TRADE_DATE'] > last_scanned_date].copy()
    print(f"Filtered data to include only records after {last_scanned_date.strftime('%Y-%m-%d')}.")
else:
    df_filtered = df.copy()
    print("No LastDateScanned found. Processing entire dataset for the first time.")

if df_filtered.empty:
    print("No new data to process. Exiting incremental update.")
else:
    print(f"Found {len(df_filtered)} new records to process.")

    # 6. Transform Data for New CSV Format
    df_filtered['Date'] = df_filtered['TRADE_DATE'].dt.strftime('%d%m%Y')
    df_filtered['DelPerc'] = df_filtered['DELIV_PER']

    # Select only 'SYMBOL', 'Date', and 'DelPerc' columns
    df_transformed = df_filtered[['SYMBOL', 'Date', 'DelPerc']]

    print("Data transformed for individual stock CSVs.")

    # 7. Generate and Append Individual Stock CSVs
    grouped = df_transformed.groupby('SYMBOL')

    for symbol, symbol_df in grouped:
        output_file_path = os.path.join(chart_dir, f'{symbol}.csv')

        if os.path.exists(output_file_path):
            # Load existing data
            existing_df = pd.read_csv(output_file_path, dtype={'Date': str, 'DelPerc': float})

            # Concatenate new and existing data
            # New data (symbol_df) takes precedence in case of duplicate dates because keep='last'
            combined_df = pd.concat([existing_df, symbol_df[['Date', 'DelPerc']]]).drop_duplicates(subset=['Date'], keep='last')

            # Sort by date for chronological order
            combined_df['Date'] = pd.to_datetime(combined_df['Date'], format='%d%m%Y')
            combined_df = combined_df.sort_values(by='Date').reset_index(drop=True)
            combined_df['Date'] = combined_df['Date'].dt.strftime('%d%m%Y') # Convert back to DDMMYYYY

            combined_df.to_csv(output_file_path, index=False)
            print(f"Appended new data to '{output_file_path}'.")
        else:
            # If file doesn't exist, just save the new data
            symbol_df[['Date', 'DelPerc']].to_csv(output_file_path, index=False)
            print(f"Created new file '{output_file_path}'.")

    print(f"Generated/Appended {len(grouped)} individual stock CSVs in the '{chart_dir}' directory.")

# 8. Generate Updated Symbol Info JSON
unique_symbols = original_df_for_json['SYMBOL'].unique().tolist()
latest_trade_date = original_df_for_json['TRADE_DATE'].max()

# Format latest_trade_date to YYYY-MM-DD string for JSON
latest_trade_date_str = latest_trade_date.strftime('%Y-%m-%d')

symbol_info_data_final = {
    "symbol": unique_symbols,
    "pricescale": 2,
    "LastDateScanned": latest_trade_date_str
}

with open(symbol_info_json_path, 'w') as f:
    json.dump(symbol_info_data_final, f, indent=4)

print(f"Updated '{symbol_info_json_path}' with {len(unique_symbols)} symbols and LastDateScanned: {latest_trade_date_str}.")
print("Script execution complete.")
