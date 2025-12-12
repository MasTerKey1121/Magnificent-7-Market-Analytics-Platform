import yfinance as yf
import pandas as pd
import s3fs
import os
from datetime import datetime

# --- CONFIG ---
MINIO_ENDPOINT = "http://localhost:9000" 
MINIO_KEY = "masterkey11"
MINIO_SECRET = "123456789"
BUCKET_NAME = "stock-data"
TICKERS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA']

def init_master_data():
    
    fs = s3fs.S3FileSystem(
        key=MINIO_KEY,
        secret=MINIO_SECRET,
        client_kwargs={'endpoint_url': MINIO_ENDPOINT}
    )
    
    if not fs.exists(BUCKET_NAME):
        fs.mkdir(BUCKET_NAME)
        print(f"Created bucket: {BUCKET_NAME}")

    data = yf.download(TICKERS, period="5y", group_by='ticker')
    
    all_dfs = []
    
    # 3. เตรียมข้อมูล
    for ticker in TICKERS:
        try:
            df = data[ticker].copy()
            if df.empty: continue
            
            df.reset_index(inplace=True)
            df['Ticker'] = ticker
            

            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'])
            
            all_dfs.append(df)
            print(f"   - Processed {ticker}")
        except Exception as e:
            print(f"Error {ticker}: {e}")
            
    if all_dfs:
        master_df = pd.concat(all_dfs, ignore_index=True)
        master_df.sort_values(by=['Date', 'Ticker'], inplace=True)
        
        # 5. บันทึกลง MinIO ที่ path: stock-data/processed/master_stock_data.csv
        file_path = f"{BUCKET_NAME}/processed/master_stock_data.csv"
        
        with fs.open(file_path, 'w') as f:
            master_df.to_csv(f, index=False)
            
    else:
        pass

if __name__ == "__main__":
    init_master_data()