"""
=============================================================================
  ETL PIPELINE — Customer Retention & RFM Analytics
  Layer 1: Extract → Clean → Transform → Load (MySQL)
  Dataset: UCI Online Retail (Kaggle - carrie1)
=============================================================================
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import warnings
import logging
import os
from datetime import datetime
from dotenv import load_dotenv
from urllib.parse import quote_plus
load_dotenv()  # loads .env from project root

warnings.filterwarnings("ignore")

# ─── Logging Setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw", "ecommerce_data.csv")
PROCESSED_PATH = os.path.join(BASE_DIR, "data", "processed", "rfm_scores.csv")

raw_password = os.getenv("DB_PASSWORD")
safe_password = quote_plus(raw_password) if raw_password else ""

DB_CONFIG = {
    "user": os.getenv("DB_USER", "root"),
    "password": safe_password,
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "database": os.getenv("DB_NAME", "rfm_analytics"),
}

TABLE_NAME = "rfm_scores"


# ─── LAYER 1A: EXTRACT ────────────────────────────────────────────────────────
def extract(path: str) -> pd.DataFrame:
    """Load raw CSV into a DataFrame."""
    log.info(f"Extracting data from: {path}")
    df = pd.read_csv(path, encoding="ISO-8859-1")
    log.info(f"Raw shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    log.info(f"Columns: {df.columns.tolist()}")
    return df


# ─── LAYER 1B: CLEAN ─────────────────────────────────────────────────────────
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Business Rules Applied:
    1. Drop rows where CustomerID is null (cannot attribute transactions)
    2. Remove negative Quantity rows (product returns / data errors)
    3. Remove zero or negative UnitPrice rows (data quality issue)
    4. Remove cancelled invoices (InvoiceNo starting with 'C')
    5. Parse InvoiceDate to datetime
    """
    log.info("Starting data cleaning...")
    initial_rows = len(df)

    # Rule 1: Drop null CustomerIDs
    df = df.dropna(subset=["CustomerID"])
    log.info(f"After dropping null CustomerIDs: {len(df):,} rows (removed {initial_rows - len(df):,})")

    # Rule 2: Remove negative quantities (returns)
    df = df[df["Quantity"] > 0]
    log.info(f"After removing returns (Quantity ≤ 0): {len(df):,} rows")

    # Rule 3: Remove zero/negative unit prices
    df = df[df["UnitPrice"] > 0]
    log.info(f"After removing invalid prices: {len(df):,} rows")

    # Rule 4: Remove cancellations
    df = df[~df["InvoiceNo"].astype(str).str.startswith("C")]
    log.info(f"After removing cancelled invoices: {len(df):,} rows")

    # Rule 5: Parse dates
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])

    # Type casting
    df["CustomerID"] = df["CustomerID"].astype(int)

    total_removed = initial_rows - len(df)
    log.info(
        f"Cleaning complete. Total removed: {total_removed:,} rows ({total_removed / initial_rows:.1%} of raw data)")
    return df.reset_index(drop=True)


# ─── LAYER 1C: TRANSFORM (RFM CALCULATION) ───────────────────────────────────
def transform_rfm(df: pd.DataFrame) -> pd.DataFrame:
    """
    RFM Metric Definitions:
    - Recency   : Days since last purchase (lower = better)
    - Frequency : Number of unique invoices (higher = better)
    - Monetary  : Total revenue generated (higher = better)

    Scoring: Quantile-based 1-5 scale per metric.
    Combined RFM Score: simple sum (max = 15, min = 3)
    """
    log.info("Calculating RFM metrics...")

    # Calculate TotalPrice per line item
    df["TotalPrice"] = df["Quantity"] * df["UnitPrice"]

    # Snapshot date — one day after the last transaction in the dataset
    snapshot_date = df["InvoiceDate"].max() + pd.Timedelta(days=1)
    log.info(f"Snapshot date for Recency calculation: {snapshot_date.date()}")

    # Aggregate to customer level
    rfm = df.groupby("CustomerID").agg(
        LastPurchaseDate=("InvoiceDate", "max"),
        Frequency=("InvoiceNo", "nunique"),
        Monetary=("TotalPrice", "sum"),
    ).reset_index()

    # Recency in days
    rfm["Recency"] = (snapshot_date - rfm["LastPurchaseDate"]).dt.days
    rfm.drop(columns=["LastPurchaseDate"], inplace=True)

    log.info(f"RFM base table: {len(rfm):,} unique customers")
    log.info(f"\n{rfm[['Recency', 'Frequency', 'Monetary']].describe().round(2)}")

    # ── Quantile Scoring (1–5) ────────────────────────────────────────────────
    # Recency: LOWER is BETTER → reverse labels
    rfm["R_Score"] = pd.qcut(rfm["Recency"], q=5, labels=[5, 4, 3, 2, 1]).astype(int)

    # Frequency: HIGHER is BETTER
    rfm["F_Score"] = pd.qcut(rfm["Frequency"].rank(method="first"), q=5, labels=[1, 2, 3, 4, 5]).astype(int)

    # Monetary: HIGHER is BETTER
    rfm["M_Score"] = pd.qcut(rfm["Monetary"].rank(method="first"), q=5, labels=[1, 2, 3, 4, 5]).astype(int)

    # Combined RFM Score
    rfm["RFM_Score"] = rfm["R_Score"] + rfm["F_Score"] + rfm["M_Score"]

    # RFM Segment String (e.g., "555" for a Champion)
    rfm["RFM_Segment"] = (
            rfm["R_Score"].astype(str)
            + rfm["F_Score"].astype(str)
            + rfm["M_Score"].astype(str)
    )

    # Round monetary for readability
    rfm["Monetary"] = rfm["Monetary"].round(2)

    log.info("RFM scoring complete.")
    return rfm


# ─── LAYER 1D: LOAD (MySQL) ───────────────────────────────────────────────────
def load_to_mysql(df: pd.DataFrame, config: dict, table: str) -> None:
    """
    Load the processed RFM DataFrame into MySQL using SQLAlchemy.
    Replaces table on each run (idempotent pipeline).
    """
    conn_str = (
        f"mysql+pymysql://{config['user']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['database']}"
    )
    log.info(f"Connecting to MySQL: {config['host']}:{config['port']}/{config['database']}")

    engine = create_engine(conn_str, echo=False)

    # Ensure database exists
    with engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {config['database']}"))
        log.info(f"Database '{config['database']}' confirmed.")

    # Write DataFrame
    df.to_sql(
        name=table,
        con=engine,
        if_exists="replace",  # Drops & recreates table → idempotent
        index=False,
        chunksize=1000,
    )
    log.info(f"Successfully loaded {len(df):,} rows into `{config['database']}`.`{table}`")


# ─── MAIN ORCHESTRATOR ────────────────────────────────────────────────────────
def run_pipeline():
    log.info("=" * 60)
    log.info("  RFM ETL PIPELINE — START")
    log.info("=" * 60)

    # Extract
    raw_df = extract(RAW_DATA_PATH)

    # Clean
    clean_df = clean(raw_df)

    # Transform
    rfm_df = transform_rfm(clean_df)

    # Save processed CSV (for audit trail)
    os.makedirs(os.path.dirname(PROCESSED_PATH), exist_ok=True)
    rfm_df.to_csv(PROCESSED_PATH, index=False)
    log.info(f"Processed RFM data saved to: {PROCESSED_PATH}")

    # Load to MySQL
    load_to_mysql(rfm_df, DB_CONFIG, TABLE_NAME)

    log.info("=" * 60)
    log.info("  RFM ETL PIPELINE — COMPLETE ✓")
    log.info("=" * 60)

    return rfm_df


if __name__ == "__main__":
    rfm = run_pipeline()
    print("\nSample Output:")
    print(rfm.head(10).to_string(index=False))