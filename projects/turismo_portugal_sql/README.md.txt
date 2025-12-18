# 🏨 Portugal Tourism Data Warehouse (Star Schema)

## 📌 Project Overview
This project demonstrates the transformation of raw tourism statistics into a structured **Data Warehouse** utilizing the **Star Schema** modeling technique. 

Designed to support analytical queries regarding hospitality metrics (guests and revenue) across different Portuguese regions, moving away from flat-file analysis to relational database engineering.

## 🏗️ Architecture & Modeling
The pipeline ingests raw Excel/CSV data (simulating INE - National Statistics Institute data), cleans it using Python/Pandas, and loads it into a **SQLite** database modeled with Fact and Dimension tables.



[Image of star schema database diagram]


### Data Model (Star Schema)
* **Fact Table (`fato_turismo`):** Contains the quantitative metrics.
    * `total_hospedes` (Total Guests)
    * `proveito_total` (Total Revenue)
    * Foreign Keys to dimensions.
* **Dimension Tables:**
    * `dim_tempo`: Granularity down to Month/Year/Quarter.
    * `dim_localizacao`: Hierarchical data (Municipality -> District -> Region).

## 🛠️ Tech Stack
* **Language:** Python 3.10+
* **Data Manipulation:** Pandas & NumPy
* **Database:** SQLite (SQL Engine)
* **Architecture:** ETL (Extract, Transform, Load)

## 🚀 How to Run

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/nicolasn892/portfolio.git](https://github.com/nicolasn892/portfolio.git)
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Run the ETL Pipeline:**
    ```bash
    python src/etl_pipeline.py
    ```

4.  **Verify the Output:**
    The script will generate a `turismo_dw.db` file in the `data/processed/` folder and output a sample analytical query (Top 3 Municipalities by Revenue) in the terminal.

## 📊 Sample SQL Analysis
Once the data is loaded, you can run complex analytical queries such as:

```sql
-- Calculate Revenue per Region in 2024
SELECT 
    l.regiao,
    SUM(f.proveito_total) as total_revenue
FROM fato_turismo f
JOIN dim_localizacao l ON f.id_local = l.id_local
JOIN dim_tempo t ON f.id_tempo = t.id_tempo
WHERE t.ano = 2024
GROUP BY l.regiao;

Developed by Nicolas Martins - Data Engineer