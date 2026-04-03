"""
=============================================================================
  EXPLORATORY DATA ANALYSIS (EDA)
  Run this BEFORE etl_pipeline.py to understand your raw data.
  Outputs key stats and saves diagnostic plots.
=============================================================================
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns
import os

# ─── Config ───────────────────────────────────────────────────────────────────
RAW_PATH = os.path.join("data", "raw", "data.csv")
PLOTS_DIR = os.path.join("data", "processed", "eda_plots")
os.makedirs(PLOTS_DIR, exist_ok=True)

sns.set_theme(style="darkgrid", palette="muted")

# ─── Load ─────────────────────────────────────────────────────────────────────
df = pd.read_csv(RAW_PATH, encoding="ISO-8859-1")
df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
print(f"Shape: {df.shape}")
print(f"\nDtypes:\n{df.dtypes}")
print(f"\nMissing Values:\n{df.isnull().sum()}")
print(f"\nDescribe:\n{df.describe()}")

# ─── Plot 1: Monthly Revenue Trend ───────────────────────────────────────────
df_clean = df[df["Quantity"] > 0].copy()
df_clean = df_clean[df_clean["UnitPrice"] > 0]
df_clean["TotalPrice"] = df_clean["Quantity"] * df_clean["UnitPrice"]
df_clean["YearMonth"] = df_clean["InvoiceDate"].dt.to_period("M")

monthly = df_clean.groupby("YearMonth")["TotalPrice"].sum().reset_index()
monthly["YearMonth"] = monthly["YearMonth"].astype(str)

fig, ax = plt.subplots(figsize=(12, 5))
ax.bar(monthly["YearMonth"], monthly["TotalPrice"], color="#4FC3F7", edgecolor="white")
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f"£{x/1000:.0f}K"))
ax.set_title("Monthly Revenue Trend", fontsize=14, fontweight="bold")
ax.set_xlabel("Month")
ax.set_ylabel("Revenue")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, "monthly_revenue.png"), dpi=150)
plt.close()
print("\nPlot saved: monthly_revenue.png")

# ─── Plot 2: Top 10 Countries by Revenue ─────────────────────────────────────
country_rev = (
    df_clean.groupby("Country")["TotalPrice"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
    .reset_index()
)

fig, ax = plt.subplots(figsize=(10, 5))
sns.barplot(data=country_rev, x="TotalPrice", y="Country", palette="Blues_r", ax=ax)
ax.xaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f"£{x/1000:.0f}K"))
ax.set_title("Top 10 Countries by Revenue", fontsize=14, fontweight="bold")
ax.set_xlabel("Revenue")
ax.set_ylabel("")
plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, "top_countries.png"), dpi=150)
plt.close()
print("Plot saved: top_countries.png")

# ─── Plot 3: Customer Frequency Distribution ─────────────────────────────────
freq = df_clean.groupby("CustomerID")["InvoiceNo"].nunique()
fig, ax = plt.subplots(figsize=(10, 5))
ax.hist(freq[freq <= 50], bins=50, color="#81C784", edgecolor="white")
ax.set_title("Customer Purchase Frequency Distribution (≤50 orders)", fontsize=14, fontweight="bold")
ax.set_xlabel("Number of Unique Orders")
ax.set_ylabel("Number of Customers")
plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, "frequency_dist.png"), dpi=150)
plt.close()
print("Plot saved: frequency_dist.png")

print("\nEDA complete. All plots saved to:", PLOTS_DIR)