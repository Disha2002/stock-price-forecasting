# src/data_loader.py

import yfinance as yf
import pandas as pd
import os
import time

def fetch_stock_data(ticker: str, start_date: str, end_date: str, save_path="data/raw", retries=3, delay=5):
    """
    Fetch historical stock data from Yahoo Finance with retry and MultiIndex handling.

    Parameters:
    - ticker: str, stock symbol (e.g., "AAPL")
    - start_date: str, start date in YYYY-MM-DD
    - end_date: str, end date in YYYY-MM-DD
    - save_path: str, folder to save CSV
    - retries: int, number of times to retry if download fails
    - delay: int, seconds to wait between retries
    """

    for attempt in range(1, retries + 1):
        try:
            print(f"Downloading {ticker} data, attempt {attempt}...")
            data = yf.download(ticker, start=start_date, end=end_date, progress=False)

            if data.empty:
                print(f"No data returned for {ticker}. Attempt {attempt} of {retries}")
            else:
                # Reset index to have Date as a column
                data.reset_index(inplace=True)

                # Flatten MultiIndex columns if present
                if isinstance(data.columns, pd.MultiIndex):
                    data.columns = [col[0] if col[0] != 'Date' else 'Date' for col in data.columns]

                # Keep only relevant columns
                columns_to_keep = [col for col in ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'] if col in data.columns]
                data = data[columns_to_keep]

                # Create folder if it doesn't exist
                os.makedirs(save_path, exist_ok=True)

                # Save CSV
                file_path = os.path.join(save_path, f"{ticker}_historical.csv")
                data.to_csv(file_path, index=False)
                print(f"Data for {ticker} saved to {file_path}")

                return data

        except Exception as e:
            print(f"Attempt {attempt} failed: {e}")

        # Wait before retrying
        time.sleep(delay)

    print("All attempts failed. Please check your network or try again later.")
    return None


# Example usage
if __name__ == "__main__":
    ticker_symbol = "AAPL"
    start = "2018-01-01"
    end = "2025-01-01"
    df = fetch_stock_data(ticker_symbol, start, end)
    if df is not None:
        print(df.head())
