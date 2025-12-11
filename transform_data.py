import pandas as pd
from minio import Minio
from io import BytesIO
import os
import time
from sklearn.linear_model import LinearRegression
from datetime import timedelta

# --- CONFIG ---
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'admin')
SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'password')

SOURCE_BUCKET = "stock-data"       
DEST_BUCKET = "processed-data"     
PREDICT_BUCKET = "predictions"     

def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False
    )

def predict_future(df, years=5):
    # 1. Data Preparation
    df = df.copy() 
    df['Date_Ordinal'] = df['Date'].map(pd.Timestamp.toordinal)
    
    X = df[['Date_Ordinal']]
    y = df['Close']

    model = LinearRegression()
    model.fit(X, y)
    
    # 3. Create future dates
    last_date = df['Date'].max()
    future_days = years * 365
    future_dates = [last_date + timedelta(days=x) for x in range(1, future_days + 1)]
    
    future_df = pd.DataFrame({'Date': future_dates})
    future_df['Date_Ordinal'] = future_df['Date'].map(pd.Timestamp.toordinal)
    
    # 4. predict
    future_df['Predicted_Price'] = model.predict(future_df[['Date_Ordinal']])
    

    del future_df['Date_Ordinal']
    
    return future_df, model.coef_[0]

def transform_and_predict():
    print("🍳 Starting Data Pipeline...")
    client = get_minio_client()
    
    for bucket in [DEST_BUCKET, PREDICT_BUCKET]:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"Created bucket: {bucket}")
    objects = client.list_objects(SOURCE_BUCKET, recursive=True)
    

    for obj in objects:
        if not obj.object_name.endswith('.csv'):
            continue
            
        print(f"\nProcessing: {obj.object_name}")
        
        try:
            # --- EXTRACT ---
            response = client.get_object(SOURCE_BUCKET, obj.object_name)
            df = pd.read_csv(response)
            response.close()
            
            # --- TRANSFORM ---
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.sort_values('Date')
            
            df['SMA_20'] = df['Close'].rolling(window=20).mean()
            df['SMA_50'] = df['Close'].rolling(window=50).mean()
            df['Daily_Return_Pct'] = df['Close'].pct_change() * 100
            df['Volatility_20'] = df['Close'].rolling(window=20).std()
            
            df.dropna(inplace=True)
            
            # --- LOAD (Processed Data) ---
            csv_buffer = BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            client.put_object(
                DEST_BUCKET,
                obj.object_name,
                csv_buffer,
                length=len(csv_buffer.getvalue()),
                content_type='application/csv'
            )
            print(f"Saved processed data")

            # PREDICT
            future_df, slope = predict_future(df)
            
            # ตีความความชัน (Slope) ง่ายๆ
            trend = "Uptrend " if slope > 0 else "Downtrend "
            print(f"  Prediction ({trend}): Slope = {slope:.4f}")

            # --- LOAD (Prediction Data) ---
            pred_buffer = BytesIO()
            future_df.to_csv(pred_buffer, index=False)
            pred_buffer.seek(0)

            pred_filename = obj.object_name.replace('.csv', '_prediction.csv')
            
            client.put_object(
                PREDICT_BUCKET,
                pred_filename,
                pred_buffer,
                length=len(pred_buffer.getvalue()),
                content_type='application/csv'
            )
            print(f"Saved prediction to: {pred_filename}")
            
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    time.sleep(2)
    transform_and_predict()