import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime
import os

# Seed for reproducibility
np.random.seed(88)
fake = Faker()

# --- Config ---
NUM_RECORDS = 500
TODAY = pd.Timestamp.today().normalize()  # Consistent "daily" timestamp
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
df.to_csv(os.path.join(OUTPUT_DIR, FILENAME), index=False)
print(f"✅ {FILENAME} written to {OUTPUT_DIR}")