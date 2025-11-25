# 🚗 PakWheels Used Car ETL and Streamlit Dashboard

This project demonstrates an end-to-end data engineering pipeline, from web scraping raw data to deploying an interactive dashboard connected to a SQL database. The focus is on collecting and analyzing used car listing data from PakWheels.

## 🚀 Overview

The pipeline follows a standard **Extract, Transform, Load (ETL)** architecture using various Python libraries:

1.  **Extract:** Web scraping 10 pages of listing data from the PakWheels search page.
2.  **Transform:** Data cleaning, normalization, and visualization.
3.  **Load:** Storing the final clean dataset into a Microsoft SQL Server instance.
4.  **Deployment:** Serving the clean data and visualizations via a Streamlit web application.

## 🛠️ Technologies Used

| Category | Tool / Library | Purpose |
| :--- | :--- | :--- |
| **Scraping** | `requests`, `beautifulsoup4` | Fetching HTML content and parsing data. |
| **Data Processing** | `pandas`, `numpy` | Data cleaning, transformation, and feature engineering. |
| **Visualization** | `matplotlib` | Generating static plots (e.g., price distribution). |
| **Database** | `SQLAlchemy`, `pyodbc` | Connecting Python to and loading data into SQL Server. |
| **Deployment** | `streamlit` | Creating and hosting the interactive web dashboard. |


## ⚙️ Setup and Execution

### Prerequisites

* Python 3.8+
* Microsoft SQL Server Management Studio (SSMS)
* **ODBC Driver 17 for SQL Server** (Confirmed to be installed)

### 1. Database Setup (SQL Server)

Ensure a database named `CarDataDB` is created on your SQL Server instance (`YOUR_SERVER_NAME`).

### 2. Python Environment Setup

```bash
# Clone the repository
git clone <link>
cd pakwheels_etl_project

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\Activate.ps1 on Windows

# Install dependencies
pip install -r requirements.txt
