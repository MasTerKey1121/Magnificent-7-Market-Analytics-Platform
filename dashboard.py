import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import s3fs
import os
from datetime import timedelta

# --- 1. ตั้งค่าหน้า Dashboard ---
st.set_page_config(page_title="Magnificent 7 AI Dashboard", layout="wide", initial_sidebar_state="expanded")
st.title("📈 Magnificent 7: Stock Analysis & AI Prediction")
st.markdown("""
<style>
    .stMetric {
        background-color: #0E1117;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #262730;
    }
</style>
""", unsafe_allow_html=True)


# ในไฟล์ dashboard.py

def get_minio_fs():
    # ถ้ามีค่า MINIO_ENDPOINT ใน Environment (จาก Docker) ให้ใช้ค่านั้น
    # ถ้าไม่มี (รันในเครื่องตัวเอง) ให้ใช้ localhost
    endpoint = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
    
    return s3fs.S3FileSystem(
        key='masterkey11',          
        secret='123456789',    
        client_kwargs={'endpoint_url': endpoint}
    )

# --- 3. ฟังก์ชันโหลดข้อมูลประวัติราคา ---
@st.cache_data(ttl=60)
def load_stock_data():
    try:
        fs = get_minio_fs()
        bucket_path = "stock-data/raw_data"
        
        if not fs.exists(bucket_path): return None, f"ไม่พบ Bucket: {bucket_path}"
        
        dirs = [d for d in fs.ls(bucket_path) if fs.isdir(d)]
        if not dirs: return None, "ไม่พบข้อมูล History"
        latest_dir = sorted(dirs)[-1]
        
        files = fs.glob(f"{latest_dir}/*.csv")
        if not files: return None, "ไม่พบไฟล์ CSV ใน History"
            
        df_list = []
        for file in files:
            with fs.open(file, 'rb') as f:
                temp_df = pd.read_csv(f)
                ticker = file.split('/')[-1].replace('.csv', '').split('_')[0]
                temp_df['Ticker'] = ticker
                df_list.append(temp_df)
                
        return pd.concat(df_list, ignore_index=True), None
    except Exception as e:
        return None, str(e)

# --- 4. ฟังก์ชันโหลดข้อมูลทำนาย ---
@st.cache_data(ttl=60)
def load_prediction_data():
    try:
        fs = get_minio_fs()
        bucket_path = "predictions/raw_data"
        
        if not fs.exists(bucket_path): return None
        
        dirs = [d for d in fs.ls(bucket_path) if fs.isdir(d)]
        if not dirs: return None
        latest_dir = sorted(dirs)[-1]
        
        files = fs.glob(f"{latest_dir}/*.csv")
        if not files: return None
            
        df_list = []
        for file in files:
            with fs.open(file, 'rb') as f:
                temp_df = pd.read_csv(f)
                ticker = file.split('/')[-1].split('_')[0]
                temp_df['Ticker'] = ticker
                
                pred_col = next((c for c in temp_df.columns if 'pred' in c.lower()), None)
                if pred_col:
                    temp_df.rename(columns={pred_col: 'Predicted_Price'}, inplace=True)
                    
                df_list.append(temp_df)
                
        return pd.concat(df_list, ignore_index=True) if df_list else None
    except Exception:
        return None

# --- 5. Main Execution ---
df_hist, err = load_stock_data()
df_pred = load_prediction_data()

if err:
    st.error(f"❌ Error loading stock data: {err}")
    st.stop()
    
if df_hist is None or df_hist.empty:
    st.warning("⏳ Waiting for stock data ingestion...")
    st.stop()

# แปลงวันที่
date_col = next((c for c in df_hist.columns if 'date' in c.lower()), 'Date')
df_hist[date_col] = pd.to_datetime(df_hist[date_col])

# Sidebar
st.sidebar.header("📊 Configuration")
all_tickers = sorted(df_hist['Ticker'].unique())
selected_tickers = st.sidebar.multiselect("Select Tickers:", all_tickers, default=all_tickers[:2])

if selected_tickers:
    # --- ส่วนที่ 1: Metric Cards ---
    st.subheader("🤖 AI Price Prediction (Next Trading Day)")
    cols = st.columns(4)
    
    for i, ticker in enumerate(selected_tickers):
        hist_data = df_hist[df_hist['Ticker'] == ticker].sort_values(by=date_col)
        if hist_data.empty: continue
            
        last_close = hist_data.iloc[-1]['Close']
        
        pred_val = None
        if df_pred is not None:
            pred_row = df_pred[df_pred['Ticker'] == ticker]
            if not pred_row.empty:
                pred_val = pred_row.iloc[0]['Predicted_Price']

        with cols[i % 4]:
            if pred_val:
                diff = pred_val - last_close
                pct = (diff / last_close) * 100
                st.metric(
                    label=f"{ticker}",
                    value=f"${pred_val:,.2f}",
                    delta=f"{diff:+.2f} ({pct:+.2f}%)"
                )
            else:
                st.metric(label=ticker, value=f"${last_close:,.2f}", delta="No Prediction Data")

    # --- ส่วนที่ 2: Advanced Chart ---
    st.divider()
    st.subheader("📉 Price Trend & Forecast")
    
    max_date = df_hist[date_col].max()
    start_date = max_date - pd.DateOffset(years=5)
    
    fig = go.Figure()
    
    for ticker in selected_tickers:
        t_data = df_hist[(df_hist['Ticker'] == ticker) & (df_hist[date_col] >= start_date)].sort_values(by=date_col)
        
        # กราฟราคาจริง
        fig.add_trace(go.Scatter(
            x=t_data[date_col],
            y=t_data['Close'],
            mode='lines',
            name=f'{ticker} History'
        ))
        
        # จุดทำนาย
        if df_pred is not None:
            pred_row = df_pred[df_pred['Ticker'] == ticker]
            if not pred_row.empty:
                pred_price = pred_row.iloc[0]['Predicted_Price']
                last_hist_date = t_data.iloc[-1][date_col]
                next_date = last_hist_date + timedelta(days=1)
                
                fig.add_trace(go.Scatter(
                    x=[last_hist_date, next_date],
                    y=[t_data.iloc[-1]['Close'], pred_price],
                    mode='lines+markers',
                    line=dict(dash='dot', width=2),
                    marker=dict(symbol='star', size=10),
                    name=f'{ticker} Forecast',
                    showlegend=False
                ))

    fig.update_layout(height=600, xaxis_title="Date", yaxis_title="Price (USD)", hovermode="x unified", template="plotly_dark")
    st.plotly_chart(fig, use_container_width=True)
    
else:
    st.info("Please select tickers from the sidebar.")

# --- Debug Section (Updated Consistency) ---
with st.expander("🔍 System Inspector (Filtered Data)"):
    if not selected_tickers:
        st.write("Please select tickers to inspect data.")
    else:
        # วนลูปสร้างตารางแยกตามหุ้นที่เลือก
        for ticker in selected_tickers:
            st.markdown(f"### 📌 Data Inspector: {ticker}")
            
            c1, c2 = st.columns(2)
            
            # ตารางซ้าย: History (แสดงแค่ 5 วันล่าสุด)
            with c1:
                st.caption(f"📉 Stock History (Last 5 Days)")
                t_hist = df_hist[df_hist['Ticker'] == ticker].sort_values(by=date_col)
                st.dataframe(t_hist.tail(5), use_container_width=True)
            
            # ตารางขวา: Prediction
            with c2:
                st.caption(f"🤖 AI Prediction Data")
                if df_pred is not None:
                    t_pred = df_pred[df_pred['Ticker'] == ticker]
                    if not t_pred.empty:
                        st.dataframe(t_pred, use_container_width=True)
                    else:
                        st.info("No prediction found for this ticker.")
                else:
                    st.info("Prediction dataset is empty.")
            
            st.divider() # เส้นขีดคั่นระหว่างหุ้นแต่ละตัว