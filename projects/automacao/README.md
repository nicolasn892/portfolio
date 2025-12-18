# 🤖 Automated Data Ingestion Bot

## 📌 Project Overview
A Python automation script designed to handle the **Ingestion Layer** of a data pipeline. It standardizes, validates, and organizes raw files arriving from external sources (e.g., FTP dumps, User uploads).

Instead of manual handling, this bot automatically detects file types, performs necessary conversions (Excel to CSV), and routes them to structured directories ready for ETL processing.



## ⚡ Key Features
* **Auto-Conversion:** Automatically converts proprietary `.xlsx` files into open standard `.csv` for Data Lake compatibility.
* **Sanitization:** Renames files to remove spaces/special characters and appends timestamps for audit trails.
* **Routing:** Sorts files into `csv/`, `json/`, and `archive/` folders based on extension.
* **Robustness:** Uses Python `Pathlib` for cross-platform compatibility (Windows/Linux) and `Logging` for monitoring.

## 🛠️ How it Works
The script `organizer_bot.py` scans the `input_folder/`.
1.  **If Excel:** Converts to CSV -> Moves CSV to `output/csv` -> Moves original Excel to `output/excel_archive`.
2.  **If CSV/JSON:** Sanitizes name -> Moves to respective output folder.
3.  **Others:** Moves to `output/others`.

## 🚀 Usage
1.  Place any file inside `input_folder`.
2.  Run the bot:
    ```bash
    python src/organizer_bot.py
    ```
3.  Check `output_folder` to see the organized data pipeline.