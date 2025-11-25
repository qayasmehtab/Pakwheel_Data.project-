# src/app.py - UPDATED

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os

# --- CONFIGURATION (same as etl.py) ---
SQL_SERVER = "MYPC"
SQL_DATABASE = "CarDataDB"
SQL_DRIVER = "ODBC Driver 17 for SQL Server"
SQL_TABLE = "PakwheelsCars"

SQL_CONN_STRING = (
    f"mssql+pyodbc://{SQL_SERVER}/{SQL_DATABASE}?"
    f"driver={SQL_DRIVER}&trusted_connection=yes"
)

# File Paths
CLEAN_CSV_PATH = "data/clean_car_data.csv"
PLOT_PATH = "data/price_distribution.png"


# --- HELPER FUNCTION ---
@st.cache_data(ttl=600)
def load_data_source():
    """Try SQL first, fallback to CSV."""
    # Try SQL
    try:
        engine = create_engine(SQL_CONN_STRING)
        df = pd.read_sql(f"SELECT * FROM {SQL_TABLE}", engine)
        if not df.empty:
            st.sidebar.success("Data source: SQL Server")
            return df
        else:
            st.sidebar.warning("SQL table empty, falling back to CSV.")
    except Exception as e:
        st.sidebar.warning(f"SQL load failed, falling back to CSV. Error: {e}")

    # Fallback to CSV
    if os.path.exists(CLEAN_CSV_PATH):
        df = pd.read_csv(CLEAN_CSV_PATH)
        if not df.empty:
            st.sidebar.info("Data source: CSV")
            return df

    st.error("Data not found. Run `python src/etl.py` first.")
    return pd.DataFrame()


# --- STREAMLIT APP ---
def main():
    st.set_page_config(layout="wide", page_title="Pakwheels Dashboard")
    st.title("🚗 Pakwheels Used Car Analysis")
    st.markdown("---")

    df = load_data_source()

    if df.empty:
        st.warning("No data available. Please ensure `etl.py` ran successfully.")
        return

    # Metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Listings", f"{len(df):,}")
    col2.metric("Average Price (PKR)", f"{df['Price_PKR'].mean():,.0f}")
    col3.metric("Newest Car Year", f"{df['Year'].max():.0f}")

    st.markdown("## 📊 Data Visualization")

    # Plot
    if os.path.exists(PLOT_PATH):
        st.image(PLOT_PATH, caption="Car Price Distribution", use_column_width=True)
    else:
        st.warning("Plot not found. Run `etl.py` to generate the image.")

    st.markdown("## 🔎 Interactive Data Table")
    st.dataframe(df, use_container_width=True)


if __name__ == "__main__":
    main()
