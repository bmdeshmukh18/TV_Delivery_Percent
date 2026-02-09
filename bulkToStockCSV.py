import pandas as pd
import os
import json

# 1. Define constants
output_csv_file_name = 'data/nse_eq_combined_deliveryPerc.csv'

# 2. Load the nse_eq_combined_deliveryPerc.csv file into a pandas DataFrame
df = pd.read_csv(output_csv_file_name, parse_dates=['TRADE_DATE'], dtype={'DELIV_PER': str})

# 3. Ensure the 'DELIV_PER' column is numeric, coercing any errors to NaN, and then fill any NaN values with 0.0
df['DELIV_PER'] = pd.to_numeric(df['DELIV_PER'], errors='coerce')
df['DELIV_PER'] = df['DELIV_PER'].fillna(0.0)

# 4. Create the `data` directory if it does not already exist
os.makedirs('data', exist_ok=True)
print("Ensured 'data' directory exists.")

# 5. Create the `symbol_info` directory if it does not already exist
os.makedirs('symbol_info', exist_ok=True)
print("Ensured 'symbol_info' directory exists.")

print("Data loaded and prepared. Directories created.")

# 6. Transform Data for OHLCV Format
# Create a new column named `date` by formatting `TRADE_DATE` as YYYYMMDDT
df['date'] = df['TRADE_DATE'].dt.strftime('%Y%m%dT')

# Create new columns named `open`, `high`, `low`, and `close` and set their values to `DELIV_PER`
df['open'] = df['DELIV_PER']
df['high'] = df['DELIV_PER']
df['low'] = df['DELIV_PER']
df['close'] = df['DELIV_PER']

# Create a new column named `volume` and set its value to 0
df['volume'] = 0

# Select only the required columns for stock CSVs
df_ohlcv = df[['SYMBOL', 'date', 'open', 'high', 'low', 'close', 'volume']]

print("Data transformed to OHLCV format.")

# 7. Generate Individual Stock CSVs
grouped = df_ohlcv.groupby('SYMBOL')

for symbol, symbol_df in grouped:
    output_file_path = os.path.join('data', f'{symbol}.csv')
    # Save the DataFrame to CSV, ensuring the correct columns are saved in the specified order
    symbol_df[['date', 'open', 'high', 'low', 'close', 'volume']].to_csv(output_file_path, index=False)

print(f"Generated {len(grouped)} individual stock CSVs in the 'data' directory.")

# 8. Generate Single Symbol Info JSON
unique_symbols = df['SYMBOL'].unique().tolist()

symbol_info_data = {
    "symbol": unique_symbols,
    "pricescale": 2,
    "description": unique_symbols # Using symbol name as description as per the example
}

output_json_file = os.path.join('symbol_info', 'symbol_info.json')

with open(output_json_file, 'w') as f:
    json.dump(symbol_info_data, f, indent=4)

print(f"'symbol_info.json' created successfully with {len(unique_symbols)} entries in the 'symbol_info' directory.")

print("Script execution complete.")
