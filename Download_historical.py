import yfinance as yf
import pandas as pd
import datetime
import os
from tqdm import tqdm
import requests_cache
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter


class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass
session = CachedLimiterSession(
    limiter=Limiter(RequestRate(2, Duration.SECOND*5)),  # max 2 requests per 5 seconds
    bucket_class=MemoryQueueBucket,
    backend=SQLiteCache("yfinance.cache"),
)

session = requests_cache.CachedSession('yfinance.cache')
session.headers['User-agent'] = 'my-program/1.0'

## Define function to extract historical data for each ticker
def fetch_data(ticker_symbol):
    ticker = yf.Ticker(ticker_symbol)
    today = datetime.date.today().strftime('%Y-%m-%d')
    data = ticker.history(start="2015-01-01", end=today)
    data.reset_index(inplace=True)
    
    # Check if the 'Date' column is of datetime type
    if pd.api.types.is_datetime64_any_dtype(data['Date']):
        data['Date'] = data['Date'].dt.strftime('%Y-%m-%d')
    
    # Drop the unnecessary columns
    data.drop(columns=['Dividends', 'Stock Splits', 'Adj Close'], inplace=True, errors='ignore')
    
    data['ticker'] = ticker_symbol  # Convert to lowercase
    
    # Convert other column names to lowercase
    data.columns = [col.lower() for col in data.columns]

    return data

## Extract all symbols
symbols= pd.read_csv("tickers_nse500.txt", header=0, sep='\t', engine="python",
                     names=("Company", "Industry", "Symbol", "Series", "ISIN"))

NSE = symbols.Symbol + ".NS"

'''
#Loop over each symbol and append data to the master DataFrame
all_data = []
for symbol in tqdm(NSE, desc="Fetching data"):  # tqdm progress bar!
    all_data.append(fetch_data(symbol))

# Concatenate all the individual datasets into one
master_data = pd.concat(all_data, ignore_index=True)

#Save the master DataFrame as a CSV
master_data.to_csv('all_NSE_symbols_data.csv', index=False)
'''

# Save individual CSVs for each index
# Create a directory in the output to store individual datasets
output_dir = 'individual_indices_data_NSE500'
if not os.path.exists(output_dir):
    os.mkdir(output_dir)

for symbol in tqdm(NSE, desc="Saving individual datasets"):
    #print(symbol)
    # tqdm progress bar!
    single_data = fetch_data(symbol)
    
    # Drop the 'ticker' column if it exists
    if 'ticker' in single_data.columns:
        single_data.drop('ticker', axis=1, inplace=True)
    
    single_data.to_csv(f'{output_dir}/{symbol}_data.csv', index=False)