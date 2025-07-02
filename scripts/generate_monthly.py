import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime
import os
import boto3
from botocore.exceptions import NoCredentialsError

# --- Setup ---
fake = Faker()
OUTPUT_DIR = 'data/raw/monthly/'
TODAY = pd.Timestamp.today()
YEAR = TODAY.year
MONTHS_BACK = 1  # simulate last month
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Generate Monthly Dates (first day of each month) ---
dates = pd.date_range(
    start=pd.Timestamp(year=YEAR, month=TODAY.month, day=1) - pd.DateOffset(months=MONTHS_BACK - 1),
    periods=MONTHS_BACK,
    freq='MS'  # month start
)

# --- Static Data ---
companies = ['Spendora', 'Expensivus', 'ClarityLedger', 'TrueSpend', 'Fintrix', 'Procuro', 'LedgrIQ', 'Zentro']

software_subscriptions = {
    'Slack': 120,
    'Zoom': 80,
    'Adobe': 150,
    'AWS': 4000,
    'Snowflake': 10000,
    'Notion': 100
}

software_assignments = {
    'Engineering': ['Slack', 'Zoom', 'AWS', 'Snowflake'],
    'Marketing': ['Slack', 'Notion', 'Adobe'],
    'Sales': ['Zoom', 'Slack'],
    'Finance': ['Slack', 'Notion'],
    'HR': ['Slack', 'Notion']
}

# --- Generate Recurring Rows ---
recurring_rows = []
for company in companies:
    for dept, tools in software_assignments.items():
        for tool in tools:
            amount = software_subscriptions[tool]
            employee = fake.name()  # assigned billing contact

            for bill_date in dates:
                recurring_rows.append([
                    employee,
                    company,
                    dept,
                    'Software',
                    tool,
                    amount,
                    bill_date.date(),
                    'subscription'  # corrected spelling
                ])

# --- Save to CSV ---
df_monthly = pd.DataFrame(recurring_rows, columns=[
    'employee', 'company', 'department', 'category',
    'merchant', 'amount', 'date', 'type'
])

FILENAME = f"monthly_recurring_{TODAY.strftime('%Y-%m')}.csv"
local_path = os.path.join(OUTPUT_DIR, FILENAME)

# Save locally for testing
df_monthly.to_csv(local_path, index=False)
print(f"✅ {FILENAME} written to {OUTPUT_DIR}")

# S3 upload
bucket_name = "money-mop"
s3_key = f"monthly-subscriptions-raw/{FILENAME}"

try:
    s3 = boto3.client("s3")
    s3.upload_file(local_path, bucket_name, s3_key)
    print(f"✅ Uploaded to s3://{bucket_name}/{s3_key}")
except NoCredentialsError:
    print("❌ AWS credentials not found. File was not uploaded.")
