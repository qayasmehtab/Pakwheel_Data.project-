# src/etl.py - UPDATED

import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import time
import matplotlib.pyplot as plt
import re


# --- CONFIGURATION ---
SQL_SERVER = "MYPC"
SQL_DATABASE = "CarDataDB"
SQL_DRIVER = "ODBC Driver 17 for SQL Server"
SQL_TABLE = "PakwheelsCars"

SQL_CONN_STRING = (
    f"mssql+pyodbc://{SQL_SERVER}/{SQL_DATABASE}?"
    f"driver={SQL_DRIVER}&trusted_connection=yes"
)

# File Paths
BASE_URL = "https://www.pakwheels.com/used-cars/search/-/page"
TOTAL_PAGES = 10
RAW_CSV_PATH = "data/raw_car_data.csv"
CLEAN_CSV_PATH = "data/clean_car_data.csv"
PLOT_PATH = "data/price_distribution.png"


# ---------- HELPERS ----------

def parse_price(price_str):
    """
    'PKR 40.75 lacs'  -> 4,075,000
    'PKR 1.21 crore'  -> 12,100,000
    'PKR 850,000'     -> 850000
    """
    s = str(price_str).lower().replace("pkr", "").strip()
    s = s.replace(",", "")

    if "crore" in s:
        nums = re.findall(r"[\d\.]+", s)
        return float(nums[0]) * 10000000 if nums else None

    if "lac" in s or "lacs" in s:
        nums = re.findall(r"[\d\.]+", s)
        return float(nums[0]) * 100000 if nums else None

    digits = re.findall(r"\d+", s)
    return float("".join(digits)) if digits else None


# ---------------------------------------------------------
# 1. EXTRACT (SCRAPING)
# ---------------------------------------------------------
def scrape_pakwheels():
    print("--- 1. Starting Scraping ---")
    all_car_data = []

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/134.0 Safari/537.36"
        ),
        "Referer": "https://www.google.com/",
        "Accept-Language": "en-US,en;q=0.9",
    }

    for page in range(1, TOTAL_PAGES + 1):
        url = BASE_URL + str(page)
        print(f"  > Scraping Page: {page}/{TOTAL_PAGES}")

        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
        except Exception as e:
            print(f"  > Error fetching {url}: {e}")
            continue

        soup = BeautifulSoup(response.text, "html.parser")

        # Multiple fallback selectors
        listings = soup.select("li.classified-listing div.result-item")
        if not listings:
            listings = soup.select("li.classified-listing")
        if not listings:
            listings = soup.select("div.search-results-row")
        if not listings:
            listings = soup.find_all("li", {"data-listing-id": True})

        if not listings:
            print(
                f"  > No listings found on page {page}. "
                "Check selectors or layout."
            )
            break

        for listing in listings:
            try:
                # Title
                title_tag = listing.find("a", class_="car-name")
                title = title_tag.text.strip() if title_tag else "N/A"

                # Price
                price_tag = listing.find("div", class_="price-details")
                price = price_tag.text.strip() if price_tag else "N/A"

                # City
                               # City (multiple fallbacks)
                city = "N/A"

                # 1) Old layout: <li class="car-location">
                city_tag = listing.find("li", class_="car-location")
                if city_tag and city_tag.text.strip():
                    city = city_tag.text.strip()
                else:
                    # 2) New layout: city mostly last text part, e.g. "Honda City 2022 PKR 4,450,000 Lahore"
                    # Try to find any element that looks like a known city name
                    possible = listing.find(
                        lambda tag: tag.name in ["span", "div", "li"]
                        and tag.get_text(strip=True)
                    )
                    if possible:
                        text = possible.get_text(" ", strip=True)
                        # common cities list (basic but works)
                        cities = [
                            "karachi", "lahore", "islamabad", "rawalpindi",
                            "peshawar", "multan", "faisalabad", "gujranwala",
                            "sialkot", "quetta", "hyderabad"
                        ]
                        for c in cities:
                            if c in text.lower().split():
                                city = c.capitalize()
                                break

                # Year & Mileage (simple fallback)
                details = listing.find_all("li")
                year = details[1].text.strip() if len(details) > 1 else "N/A"
                mileage = details[2].text.strip() if len(details) > 2 else "N/A"

                all_car_data.append(
                    {
                        "Title": title,
                        "Price_Raw": price,
                        "Year_Raw": year,
                        "Mileage_Raw": mileage,
                        "City": city,
                    }
                )
            except Exception:
                continue

        time.sleep(1.5)

    df_raw = pd.DataFrame(all_car_data)
    df_raw.to_csv(RAW_CSV_PATH, index=False)
    print(f"✅ Raw data saved: {RAW_CSV_PATH}")
    print("Raw rows scraped:", len(df_raw))
    return df_raw


# ---------------------------------------------------------
# 2. TRANSFORM (CLEANING + PLOT)
# ---------------------------------------------------------
def clean_and_visualize(df):
    print("--- 2. Cleaning & Visualization ---")

    # -------- Price ----------
    df["Price_PKR"] = df["Price_Raw"].apply(parse_price)

    # -------- Year (from Title, not Year_Raw) ----------
    # Example Title: "Honda Vezel  2014 Hybrid Z for Sale"
    df["Year"] = (
        df["Title"]
        .astype(str)
        .str.extract(r"(\d{4})")[0]   # pehla 4-digit number
        .astype(float)
    )

    # -------- Mileage ----------
    df["Mileage"] = (
        df["Mileage_Raw"]
        .astype(str)
        .str.replace(r"[^\d]", "", regex=True)
        .replace("", pd.NA)
        .astype(float)
    )

    # -------- Brand ----------
    df["Brand"] = df["Title"].apply(
        lambda x: x.split()[0] if isinstance(x, str) and x.strip() else "Unknown"
    )

    # -------- Row filter ----------
    # sirf woh rows drop karo jahan price ya year missing ho
    df_final = df.dropna(subset=["Price_PKR", "Year"])
    df_final = df_final[["Title", "Brand", "Price_PKR", "Year", "Mileage", "City"]]

    print("Rows after cleaning:", len(df_final))
    df_final.to_csv(CLEAN_CSV_PATH, index=False)
    print(f"✅ Clean CSV saved: {CLEAN_CSV_PATH}")

    # -------- Plot ----------
    if not df_final["Price_PKR"].empty:
        plt.figure(figsize=(10, 6))
        plt.hist(
            df_final["Price_PKR"],
            bins=50,
            color="indianred",
            edgecolor="black",
            log=True,
        )
        plt.title("Car Price Distribution")
        plt.xlabel("Price (PKR)")
        plt.ylabel("Count (log)")
        plt.savefig(PLOT_PATH)
        plt.close()
        print(f"✅ Plot saved: {PLOT_PATH}")
    else:
        print("⚠️ No price data to plot.")

    return df_final



# ---------------------------------------------------------
# 3. LOAD INTO SQL
# ---------------------------------------------------------
def load_to_sql(df):
    print("--- 3. Loading into SQL ---")
    try:
        engine = create_engine(SQL_CONN_STRING)
        df.to_sql(SQL_TABLE, engine, if_exists="replace", index=False)
        print(f"✅ Successfully loaded {len(df)} rows into SQL table '{SQL_TABLE}'")
    except Exception as e:
        print(f"❌ SQL Error: {e}")


# ---------------------------------------------------------
# 4. EXECUTE
# ---------------------------------------------------------
if __name__ == "__main__":
    df_raw = scrape_pakwheels()
    if len(df_raw) > 0:
        df_clean = clean_and_visualize(df_raw)
        load_to_sql(df_clean)
    else:
        print("❌ No data scraped! Check selectors or BASE_URL.")
