import pandas as pd
import numpy as np
import os
from glob import glob
import logging

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/enrich_pipeline.log"),
        logging.StreamHandler()
    ]
)

# --- Config ---
RAW_DAILY_DIR = 'data/raw/daily'
RAW_MONTHLY_DIR = 'data/raw/monthly'
OUTPUT_DIR = 'data/processed'
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs('logs', exist_ok=True)

def get_latest_csv(path):
    files = sorted(glob(os.path.join(path, '*.csv')), reverse=True)
    if files:
        logging.info(f"Found latest file in {path}: {os.path.basename(files[0])}")
        return files[0]
    else:
        logging.warning(f"No files found in {path}")
        return None

# --- Load Files ---
daily_file = get_latest_csv(RAW_DAILY_DIR)
monthly_file = get_latest_csv(RAW_MONTHLY_DIR)

if not daily_file or not monthly_file:
    logging.error("Missing daily or monthly input files. Aborting enrichment.")
    raise FileNotFoundError("❌ Could not find daily or monthly CSVs.")

df_daily = pd.read_csv(daily_file)
df_monthly = pd.read_csv(monthly_file)
logging.info(f"Loaded {len(df_daily)} daily and {len(df_monthly)} monthly records.")

# --- Combine ---
df = pd.concat([df_daily, df_monthly], ignore_index=True)
logging.info(f"Combined total records: {len(df)}")

# --- Inject Outliers ---
outliers = df.sample(frac=0.03, random_state=42).copy()
outliers['amount'] *= 3
outliers['category'] = 'Misc'
df.update(outliers)
logging.info("Injected synthetic outliers into dataset.")

# --- Enrichment ---
df['date'] = pd.to_datetime(df['date'])
df['day_of_week'] = df['date'].dt.day_name()
df['is_weekend'] = df['day_of_week'].isin(['Saturday', 'Sunday'])
df['month'] = df['date'].dt.month

df['merchant_mean'] = df.groupby('merchant')['amount'].transform('mean')
df['merchant_std'] = df.groupby('merchant')['amount'].transform('std')

df['amount_z_score'] = (df['amount'] - df['merchant_mean']) / df['merchant_std']
df['amount_z_score'] = df['amount_z_score'].replace([np.inf, -np.inf], np.nan).fillna(0)
df['is_policy_violation'] = df['amount_z_score'] > 3

df['potential_savings'] = 0.0
df.loc[df['is_policy_violation'], 'potential_savings'] = (
    df['amount'] - (df['merchant_mean'] + 3 * df['merchant_std'])
)

num_violations = df['is_policy_violation'].sum()
total_savings = df['potential_savings'].sum()

logging.info(f"Detected {num_violations} potential policy violations.")
logging.info(f"Estimated total potential savings: ${total_savings:,.2f}")

# --- Save ---
output_file = os.path.join(OUTPUT_DIR, f'enriched_transactions_{pd.Timestamp.today().strftime("%Y-%m-%d")}.csv')
df.to_csv(output_file, index=False)
logging.info(f"✅ Enriched data written to {output_file}")
