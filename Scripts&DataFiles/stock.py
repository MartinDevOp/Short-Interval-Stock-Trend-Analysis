##Requirements
"""
1) Install pip
    For Mac/Linux
    ---------------
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
    python3 get-pip.py

    For Windows
    ------------
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
    python get-pip.py

    ---------------------------------------------------------
    verify
    ---------------------------------------------------------
    pip --version
    # or
    python3 -m pip --version

2) Install python libraries
    pip install yfinance
    python3 -m pip install pandas
    pip install --upgrade pandas--> to verify installation
"""

import yfinance as yf
import pandas as pd
import os

file_name = 'StockPrices.csv'

##Read selected market stocks from csv
def Read_Stocks():
    rd = pd.read_csv('StockTickers.csv')
    numOfStock = len(rd['Stocks']) #selects column containing stocks
    print(f'There are {numOfStock} stocks to be analyzed') #Prints number of stocks to be analzed
    #Asssign as tickers as lists
    ticks = [rd['Stocks'].iloc[x].strip() for x in range(0, 4)]
    #print tickers
    print(ticks)
    return ticks

def Store_Data(ticks):
    """"
    Old Code--> This is for future review and evaluation
    ----------------------------------------------------
    tickers = yf.Tickers(ticks)
    iterate = 1
    #val = ticks[0]
    #Now get each stock information
    while iterate < len(ticks):
        data = tickers.tickers[ticks[iterate]].history(period="1mo")
        #Adds a new column
        data['Ticker'] = [ticks[iterate] for x in range(0,len(data['Open']))] 
    #data = tickers.tickers[iterate].info
    print(data)
    #row_df.to_csv(file_name, mode='a', header=not file_exists, index=False)
    #file_exists = True  # After first write, always skip header
        #print("Reading data for {data}...")
    """
     # Check if file exists
    file_exists = os.path.isfile(file_name)

#Iterates through tickers
    for ticker in ticks:
        print(f"Fetching data for {ticker}...")
        try:
            data = yf.Ticker(ticker).history(period="1mo")
            #checks if ticker information is present
            if data.empty:
                print(f"  [!] No data found for {ticker}")
                continue

            # Add a column for the ticker name
            data['Ticker'] = ticker

            # Save to CSV (append mode)
            data.to_csv(file_name, mode='a', header=not file_exists, index=True)
            file_exists = True  # Set header to False after first write

        except Exception as e:
            print(f"  [ERROR] Could not fetch data for {ticker}: {e}")

#Calling function to read all stock items from 'StockTickers.csv'
ticks = Read_Stocks()
#Storing data in csv
Store_Data(ticks)
