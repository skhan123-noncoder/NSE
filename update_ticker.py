import os
import pandas as pd
import datetime
import yfinance as yf


def update_data(file_name, ticker_symbol):
    # Read data into a data-frame
    df = pd.read_csv(file_name)

    # Get the last entry and increase by 1 day
    start_date = datetime.datetime.strptime(((df.iloc[-1]).Date), '%Y-%m-%d')
    start_date += datetime.timedelta(days=1)
    
    # Get today's date
    today = datetime.date.today().strftime('%Y-%m-%d')

    #Check if the data is already imported
    if start_date.strftime('%Y-%m-%d') == datetime.date.today().strftime('%Y-%m-%d'):
        return(0)
    else:
        ticker = yf.Ticker(ticker_symbol)
        data = ticker.history(start=start_date, end=datetime.date.today().strftime('%Y-%m-%d'))
        data.reset_index(inplace=True)

    # Check if the 'Date' column is of datetime type
        if pd.api.types.is_datetime64_any_dtype(data['Date']):
            data['Date'] = data['Date'].dt.strftime('%Y-%m-%d')

    #Add the new rows to the existing df
        df_new = pd.concat([df, data], ignore_index=True)
    
        return(df_new)

if __name__=='__main__':
    update_data()