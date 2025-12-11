import yfinance as yf
import pandas as pd
from datetime import datetime
import os
from minio import Minio
from io import BytesIO
import time

#CONFIG 
TICKERS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA']
BUCKET_NAME = os.getenv('BUCKET_NAME', 'stock-data')


MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'admin')
SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'password')

def get_minio_client():
    time.sleep(5) 
    return Minio(
        MINIO_ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False
    )

def upload_to_minio(client, data_bytes, destination_path):
    try:
        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)
            print(f"Created bucket: {BUCKET_NAME}")

        client.put_object(
            BUCKET_NAME,
            destination_path,
            data_bytes,
            length=len(data_bytes.getvalue()),
            content_type='application/csv'
        )
        print(f"Uploaded: {destination_path}")
    except Exception as e:
        print(f"Upload Failed: {e}")

def fetch_and_upload():
    print(f"[{datetime.now()}] Starting Job...")
    client = get_minio_client()
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    data = yf.download(TICKERS, period="1y", group_by='ticker')
    
    for ticker in TICKERS:
        try:
            df = data[ticker].copy()
            if df.empty: continue
            
            df.reset_index(inplace=True)
            
            csv_buffer = BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            path = f"raw_data/{today_str}/{ticker}.csv"
            upload_to_minio(client, csv_buffer, path)
            
        except Exception as e:
            print(f"Error {ticker}: {e}")

if __name__ == "__main__":
    fetch_and_upload()