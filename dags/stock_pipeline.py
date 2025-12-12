from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
import s3fs
import os

# --- CONFIGURATION ---
MINIO_ENDPOINT = "http://minio:9000" 
MINIO_KEY = "masterkey11"
MINIO_SECRET = "123456789"
BUCKET_NAME = "stock-data"
TICKERS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA']

MASTER_FILE_PATH = f"{BUCKET_NAME}/processed/master_stock_data.csv"

# ตั้งค่า Default Arguments ของ DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1), 
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- FUNCTIONS ---

def get_fs():
    """สร้าง connection ไปยัง MinIO ผ่าน s3fs"""
    return s3fs.S3FileSystem(
        key=MINIO_KEY,
        secret=MINIO_SECRET,
        client_kwargs={'endpoint_url': MINIO_ENDPOINT}
    )

def ingest_daily_data(**kwargs):
    execution_date = kwargs['ds'] 
    
    fs = get_fs()
    

    data = yf.download(TICKERS, period="5d", group_by='ticker')
    
    if not fs.exists(BUCKET_NAME):
        fs.mkdir(BUCKET_NAME)

    saved_files = []
    
    for ticker in TICKERS:
        try:
            df = data[ticker].copy()
            if df.empty: continue
            
            df.reset_index(inplace=True)
            
            df['Ticker'] = ticker
            
            file_path = f"{BUCKET_NAME}/raw_data/{execution_date}/{ticker}.csv"
            
            with fs.open(file_path, 'w') as f:
                df.to_csv(f, index=False)
            
            saved_files.append(file_path)
            
        except Exception as e:
            print(f"Error fetching {ticker}: {e}")
            
    print(f"Ingested {len(saved_files)} files.")

def transform_and_merge(**kwargs):
    execution_date = kwargs['ds']
    print(f"Starting Transform & Merge for date: {execution_date}")
    
    fs = get_fs()
    
    daily_path = f"{BUCKET_NAME}/raw_data/{execution_date}/*.csv"
    new_files = fs.glob(daily_path)
    
    if not new_files:
        return

    df_new_list = []
    for file in new_files:
        with fs.open(file, 'rb') as f:
            df_new_list.append(pd.read_csv(f))
            
    df_new = pd.concat(df_new_list, ignore_index=True)
    if 'Date' in df_new.columns:
        df_new['Date'] = pd.to_datetime(df_new['Date'])
    
    if fs.exists(MASTER_FILE_PATH):
        with fs.open(MASTER_FILE_PATH, 'rb') as f:
            df_master = pd.read_csv(f)
            if 'Date' in df_master.columns:
                df_master['Date'] = pd.to_datetime(df_master['Date'])
    else:
        df_master = pd.DataFrame()

    if not df_master.empty:
        df_combined = pd.concat([df_master, df_new], ignore_index=True)
    else:
        df_combined = df_new

# de-duplication
    initial_len = len(df_combined)
    df_combined.drop_duplicates(subset=['Date', 'Ticker'], keep='last', inplace=True)
    final_len = len(df_combined)
    
    print(f"Merged: {initial_len} rows -> Deduplicated: {final_len} rows")

    df_combined.sort_values(by=['Date', 'Ticker'], inplace=True)
    
    with fs.open(MASTER_FILE_PATH, 'w') as f:
        df_combined.to_csv(f, index=False)
        
    print(f"Successfully updated Master file at: {MASTER_FILE_PATH}")

# --- DAG DEFINITION ---

with DAG(
    'magnificent7_incremental_pipeline',
    default_args=default_args,
    description='Fetch daily stock data and merge into master dataset',
    schedule_interval='0 2 * * 1-5',
    catchup=False
) as dag:

    t1_ingest = PythonOperator(
        task_id='ingest_daily_stock',
        python_callable=ingest_daily_data,
        provide_context=True
    )

    t2_transform = PythonOperator(
        task_id='merge_to_master',
        python_callable=transform_and_merge,
        provide_context=True
    )

    # ETL Flow
    t1_ingest >> t2_transform