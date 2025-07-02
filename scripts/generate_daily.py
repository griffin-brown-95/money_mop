import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime
import os
import boto3
from botocore.exceptions import NoCredentialsError

# Seed for reproducibility
np.random.seed(88)
fake = Faker()

# --- Config ---
NUM_RECORDS = np.random.randint(400, 600)
TODAY = pd.Timestamp.today().normalize() - pd.Timedelta(days=1)  # Consistent "daily" timestamp
OUTPUT_DIR = 'data/raw/daily/'
FILENAME = f'daily_transactions_{TODAY.strftime("%Y-%m-%d")}.csv'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Static Data ---
companies = ['Spendora', 'Expensivus', 'ClarityLedger', 'TrueSpend', 'Fintrix', 'Procuro', 'LedgrIQ', 'Zentro']
departments = ['Engineering', 'Marketing', 'Sales', 'Finance', 'HR', 'Operations']
categories = ['Travel', 'Meals', 'Supplies', 'Entertainment', 'Misc']
merchants = {
    'Travel': ['Delta', 'Uber', 'Lyft', 'Marriott', 'Hilton'],
    'Meals': ['Starbucks', 'Chipotle', 'Panera', 'Olive Garden'],
    'Supplies': ['Staples', 'Office Depot', 'Amazon'],
    'Entertainment': ['AMC', 'TopGolf', 'Dave & Buster\'s'],
    'Misc': ['Etsy', 'Other', 'Unknown']
}
category_amounts = {
    'Travel': (500, 150),
    'Meals': (40, 10),
    'Supplies': (50, 20),
    'Entertainment': (100, 50),
    'Misc': (75, 75)
}

# --- Generation ---
data = []
for _ in range(NUM_RECORDS):
    company = random.choice(companies)
    department = random.choice(departments)
    category = random.choice(categories)
    merchant = random.choice(merchants[category])
    mean, std = category_amounts[category]
    amount = round(max(1, np.random.normal(loc=mean, scale=std)), 2)
    employee = fake.name()
    
    data.append([
        employee, company, department, category,
        merchant, amount, TODAY, 'transaction'
    ])

# --- Create DataFrame & Export ---
df = pd.DataFrame(data, columns=[
    'employee', 'company', 'department', 'category',
    'merchant', 'amount', 'date', 'type'
])

local_path = os.path.join(OUTPUT_DIR, FILENAME)
df.to_csv(local_path, index=False)
print(f"✅ {FILENAME} written to {OUTPUT_DIR}")

# Define S3 target
bucket_name = "money-mop"
s3_key = f"daily-transactions-raw/{FILENAME}"

try:
    s3 = boto3.client('s3')
    s3.upload_file(local_path, bucket_name, s3_key)
    print(f"✅ Uploaded to s3://{bucket_name}/{s3_key}")
except NoCredentialsError:
    print("❌ AWS credentials not found. File was not uploaded.")