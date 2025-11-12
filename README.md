# 🚀 End-to-End ETL Pipeline on GCP with Airflow, Dataflow & Data Quality Validation

### 🧩 Project Overview

This project demonstrates a **production-grade ETL pipeline** orchestrated using **Google Cloud Composer (Airflow)**.  
It automates data ingestion, transformation, and validation across **Google Cloud Platform (GCP)** services.

The pipeline extracts real-time cryptocurrency prices from a public API, stores raw data in **Google Cloud Storage (GCS)**, transforms it using **Apache Beam** on **Dataflow**, loads it into **BigQuery**, and performs **data quality checks** with **Great Expectations**.

---

## 🏗️ Architecture Diagram

    ┌───────────────────┐
    │   API Source       │
    │ (CoinGecko)        │
    └─────────┬─────────┘
              │
              ▼
    ┌───────────────────┐
    │ Cloud Composer     │
    │ (Airflow DAG)      │
    └─────────┬─────────┘
              │
              ▼
    ┌───────────────────┐
    │ Google Cloud       │
    │ Storage (Raw Data) │
    └─────────┬─────────┘
              │
              ▼
    ┌───────────────────┐
    │ Apache Beam on     │
    │ Dataflow (ETL)     │
    └─────────┬─────────┘
              │
              ▼
    ┌───────────────────┐
    │ BigQuery (Cleaned) │
    │ Partitioned Table  │
    └─────────┬─────────┘
              │
              ▼
    ┌───────────────────┐
    │ Great Expectations │
    │ Data Validation    │
    └───────────────────┘


---

## 🎯 Problem Statement

Organizations often receive data from multiple external APIs and need to **automate ingestion, transformation, and validation** before loading it into their data warehouse.  
Manual handling causes data inconsistency and delays in analytics.

---

## 💡 Solution

This pipeline automates the **end-to-end data lifecycle**:
1. **Extract** daily crypto prices from the CoinGecko API  
2. **Load** raw JSON files into **Google Cloud Storage (GCS)**  
3. **Transform** and clean data using **Apache Beam** on **Google Cloud Dataflow**  
4. **Load** clean, structured data into a **partitioned BigQuery table**  
5. **Validate** using **Great Expectations** to ensure data quality  
6. **Orchestrate** and schedule the entire workflow via **Cloud Composer (Airflow)**  

---

## ⚙️ Tech Stack

| Layer | Technology | Description |
|:------|:------------|:-------------|
| **Orchestration** | Cloud Composer (Airflow) | Schedules & manages all pipeline tasks |
| **Storage** | Google Cloud Storage (GCS) | Stores raw API data |
| **Transformation** | Apache Beam (Dataflow Runner) | Performs ETL and writes to BigQuery |
| **Data Warehouse** | BigQuery | Stores cleaned, structured data |
| **Data Validation** | Great Expectations | Ensures schema and data quality |
| **Monitoring** | Airflow UI, Email Alerts | Tracks pipeline health |

---

