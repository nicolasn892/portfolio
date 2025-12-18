# ⛈️ Portugal Weather Data Pipeline

## 📌 Project Overview
An end-to-end **ETL (Extract, Transform, Load) Pipeline** engineered to ingest real-time meteorological data from IPMA (Portuguese Institute for Sea and Atmosphere).

Unlike static CSV analysis, this project demonstrates a production-ready architecture capable of handling dynamic data sources, robust error handling, and scalable data processing using **Apache Spark**.

### 🏗️ Architecture
1.  **Extract:** Python script consumes the IPMA REST API to fetch daily forecast data (JSON).
2.  **Raw Layer (Landing Zone):** Saves raw JSON files with timestamp versioning for auditability and replayability.
3.  **Transform:** **PySpark** processes the semi-structured JSON:
    * Explodes nested arrays.
    * Applies schema validation and type casting.
    * Generates partition columns (`data_previsao`).
4.  **Load:** Writes optimized **Parquet** files partitioned by date, simulating a Data Lake structure.

## 🛠️ Tech Stack
* **Language:** Python 3.12
* **Processing Engine:** Apache Spark (PySpark)
* **Data Source:** REST API
* **Storage Format:** Parquet (Columnar Storage)
* **Environment:** Windows (with custom compatibility patches) / Linux ready

## 🚀 How to Run
1. Install dependencies:
   ```bash
   pip install pyspark requests