# 📈 Magnificent 7 Stock Pipeline & Dashboard

An end-to-end Automated Data Engineering Pipeline for analyzing the "Magnificent 7" stocks (AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA).

This project automates the extraction, transformation, and loading (ETL) of stock market data using **Apache Airflow**, stores data in a **MinIO Data Lake**, and visualizes trends and AI-based predictions via a **Streamlit Dashboard**.

![Python](https://img.shields.io/badge/Python-3.9-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10.2-017CEE)
![MinIO](https://img.shields.io/badge/MinIO-Object%20Storage-C72C48)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B)

## 🏗️ System Architecture

The system is fully containerized using Docker and consists of the following components:

```mermaid
graph LR
    A[Apache Airflow] -->|Fetch Daily Data| B(MinIO Data Lake)
    B -->|Transform & Merge| C[Master Dataset]
    D[ML Model] -->|Generate Predictions| B
    E[Streamlit Dashboard] -->|Read Data via s3fs| B
    C --> E
