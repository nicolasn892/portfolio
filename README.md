# Nicolas Martins | Data Engineering Portfolio

[![Portfolio Status](https://img.shields.io/badge/Status-Open_to_Work-green?style=for-the-badge)](https://www.linkedin.com/in/nicolas-martins-silva)
[![Portfolio Website](https://img.shields.io/badge/View_Portfolio-Website-blue?style=for-the-badge)](https://nicolasn892.github.io/portfolio/)

**Computer Engineer & Data Engineer** based in Portugal 🇵🇹.
Specialized in building scalable **ETL Pipelines**, **Data Warehousing**, and **Process Automation**.

This repository contains the source code for my portfolio projects, demonstrating practical applications of **Apache Spark**, **SQL**, **Python**, and **Cloud Concepts**.

---

## 🛠️ Tech Stack

| Domain | Technologies |
| :--- | :--- |
| **Languages** | Python (Pandas, NumPy, Requests), SQL, Bash |
| **Processing** | Apache Spark (PySpark), Databricks |
| **Database** | SQLite, PostgreSQL, Star Schema Modeling (Kimball) |
| **Tools** | Git, Docker, VS Code, Linux |
| **Format** | Parquet, JSON, CSV, Excel |

---

## 📂 Featured Projects

| Project | Tech Stack | Description | Links |
| :--- | :--- | :--- | :--- |
| **⛈️ Weather ETL Pipeline** | `PySpark` `API` `Parquet` | End-to-end pipeline ingesting real-time data from IPMA API, processing complex JSON with Spark, and loading into a Data Lake structure. | [View Code](projects/etl-spark/) |
| **🏨 Tourism Data Warehouse** | `SQL` `Star Schema` `Python` | Dimensional modeling project transforming raw stats into a Star Schema (Fact/Dimensions) for OLAP analytics. | [View Code](projects/turismo_portugal_sql/) |
| **🤖 Automated Ingestion Bot** | `Python OOP` `Automation` | Object-Oriented script to detect, sanitize, and auto-convert corporate files (Excel to CSV) for ingestion. | [View Code](projects/automacao/) |

---

## 🏗️ Repository Structure

```text
portfolio/
│
├── assets/                 # Profile images and CVs (EN/PT)
├── projects/               # Source code for all projects
│   ├── etl-spark/          # PySpark ETL Pipeline
│   ├── turismo_portugal_sql/ # SQL Data Warehouse logic
│   └── automacao/          # Python Automation Scripts
│
├── index.html              # Portfolio Website entry point
└── README.md               # You are here