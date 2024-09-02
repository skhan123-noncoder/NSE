from update_ticker import update_data as ut
from calculate_rsi import calc_rsi
import pandas as pd
import os

path = "/bi/home/khans/NSE_parent/test_datasets/"

## Update the data for each symbol by calling the update_ticker function
for each in os.listdir(path):
    file_name = path+each
    ticker_symbol = each.split("_")[0]
    updated_df = ut(file_name, ticker_symbol)
    updated_df.to_csv(f'{path}{ticker_symbol}_data.csv', index=False)


# Check the RSI for each entry in the last 60 days
## Decide the length for which you wan to calculate RSI
length = 14
final = {}

for each in os.listdir(path):    
    
    # Process data for calculating rsi
    df = pd.read_csv(path+each)
    close = df['Close']
    rsi_sma = calc_rsi(length, close, lambda s: s.rolling(length).mean())
    
    # Add data to the dictionary one by one
    final[each.split("_")[0]] = rsi_sma[-60:]

    #Store the dates here
    date_df = df['Date'][-60:].reset_index(drop=True)

rsi_df = pd.DataFrame(final).reset_index(drop=True)
rsi_df = pd.concat([date_df, rsi_df], axis=1)

rsi_df.to_csv(f'{path}/RSI_last60days.csv', index=False)


