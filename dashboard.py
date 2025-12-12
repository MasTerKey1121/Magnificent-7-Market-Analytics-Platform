import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np

# --- 1. ตั้งค่าหน้า Dashboard ---
st.set_page_config(page_title="Magnificent 7 Dashboard", layout="wide")

st.title("Magnificent 7 Stock Pipeline Dashboard And Predictions")
st.markdown("Dashboard แสดงข้อมูลราคาหุ้นและการวิเคราะห์จาก Model Scikit-learn")

# --- 2. จำลองข้อมูล (Mock Data) ---
# ในการใช้งานจริง ส่วนนี้คุณจะโหลดจากไฟล์ transform_data.py หรือไฟล์ csv ของคุณ
@st.cache_data
def load_data():
    dates = pd.date_range(start="2024-01-01", periods=100)
    # รายชื่อหุ้น Magnificent 7
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA']
    
    data = pd.DataFrame(index=dates)
    for ticker in tickers:
        # สร้างราคาจำลองแบบ Random Walk
        prices = 100 + np.cumsum(np.random.randn(100)) * 2
        data[ticker] = prices
        
    return data

df = load_data()

# --- 3. Sidebar สำหรับเลือกหุ้น ---
st.sidebar.header("User Input")
selected_tickers = st.sidebar.multiselect(
    "เลือกหุ้นที่ต้องการแสดง:",
    options=df.columns,
    default=['NVDA']
)

# --- 4. แสดงกราฟราคา (Visualization) ---
st.subheader("Stock Price Trends")

if selected_tickers:
    # เตรียมข้อมูลสำหรับ Plotly
    plot_df = df[selected_tickers].reset_index().melt('index', var_name='Ticker', value_name='Price')
    
    fig = px.line(
        plot_df, 
        x='index', 
        y='Price', 
        color='Ticker',
        title='Price History'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # --- 5. ส่วนแสดงผลลัพธ์จาก ML (จำลองส่วนที่ใช้ sklearn) ---
    st.divider()
    st.subheader("📊 Model Prediction Insights (Demo)")
    
    col1, col2, col3 = st.columns(3)
    
    # สมมติว่านี่คือค่าที่คำนวณมาจาก sklearn.linear_model
    latest_price = df[selected_tickers[0]].iloc[-1]
    predicted_next_day = latest_price * (1 + np.random.normal(0.01, 0.02)) # จำลองการทำนาย
    
    with col1:
        st.metric(
            label=f"Latest Price ({selected_tickers[0]})", 
            value=f"${latest_price:.2f}"
        )
    with col2:
        st.metric(
            label="Predicted Price (Next Day)", 
            value=f"${predicted_next_day:.2f}",
            delta=f"{predicted_next_day - latest_price:.2f}"
        )
    with col3:
        st.info("Model: LinearRegression (sklearn)")

else:
    st.warning("กรุณาเลือกหุ้นจากเมนูด้านซ้าย")

# --- 6. แสดงตารางข้อมูลดิบ ---
with st.expander("ดูข้อมูลดิบ (Raw Data)"):
    st.dataframe(df)