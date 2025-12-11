import pandas as pd
from minio import Minio
from io import BytesIO
import os
import time

# --- CONFIG ---
# เราจะตั้งค่า Connection เหมือนเดิม
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'admin')
SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'password')

SOURCE_BUCKET = "stock-data"       # ถังข้อมูลดิบ
DEST_BUCKET = "processed-data"     # ถังข้อมูลที่ปรุงเสร็จแล้ว

def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False
    )

def transform_stock_data():
    print("🍳 Starting Data Transformation...")
    client = get_minio_client()
    
    # 1. สร้างถังปลายทางถ้ายังไม่มี
    if not client.bucket_exists(DEST_BUCKET):
        client.make_bucket(DEST_BUCKET)
        print(f"📦 Created bucket: {DEST_BUCKET}")

    # 2. หาไฟล์ทั้งหมดในถัง Raw (List Objects)
    # หมายเหตุ: recursive=True เพื่อหาในโฟลเดอร์ย่อยๆ ด้วย
    objects = client.list_objects(SOURCE_BUCKET, recursive=True)
    
    for obj in objects:
        if not obj.object_name.endswith('.csv'):
            continue
            
        print(f"🔄 Processing: {obj.object_name}")
        
        try:
            # 3. EXTRACT: อ่านไฟล์จาก MinIO
            response = client.get_object(SOURCE_BUCKET, obj.object_name)
            df = pd.read_csv(response)
            response.close()
            
            # 4. TRANSFORM: คำนวณตัวเลขทางการเงิน
            # เรียงข้อมูลตามวันที่ก่อนคำนวณ
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.sort_values('Date')
            
            # คำนวณ Moving Average (เส้นค่าเฉลี่ย)
            df['SMA_20'] = df['Close'].rolling(window=20).mean()
            df['SMA_50'] = df['Close'].rolling(window=50).mean()
            
            # คำนวณ Daily Return (%)
            df['Daily_Return_Pct'] = df['Close'].pct_change() * 100
            
            # คำนวณความผันผวน (Volatility) 20 วัน
            df['Volatility_20'] = df['Close'].rolling(window=20).std()
            
            # ตัดแถวที่มีค่าว่าง (NaN) ช่วงแรกๆ ทิ้ง
            df.dropna(inplace=True)
            
            # 5. LOAD: ส่งขึ้นถังใหม่ (Processed)
            # เราจะเก็บชื่อไฟล์เดิม แต่เปลี่ยน Bucket
            # แปลง DataFrame กลับเป็น CSV Bytes
            csv_buffer = BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            # Upload
            client.put_object(
                DEST_BUCKET,
                obj.object_name, # ใช้ชื่อ path เดิม เพื่อคงโครงสร้างโฟลเดอร์ไว้
                csv_buffer,
                length=len(csv_buffer.getvalue()),
                content_type='application/csv'
            )
            print(f"✅ Saved to: {DEST_BUCKET}/{obj.object_name}")
            
        except Exception as e:
            print(f"❌ Error processing {obj.object_name}: {e}")

if __name__ == "__main__":
    # รอให้ MinIO พร้อม (เผื่อรันพร้อมกัน)
    time.sleep(5)
    transform_stock_data()