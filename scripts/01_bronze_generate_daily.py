from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import random
import uuid
from datetime import datetime

# --- Init and set seed ---
spark = SparkSession.builder.getOrCreate()
np.random.seed(88)

# --- Config ---
NUM_RECORDS = np.random.randint(400, 600)
YESTERDAY = pd.Timestamp.today().normalize() - pd.Timedelta(days=1)  # Consistent "daily" timestamp

# --- Load reference Tables ---
companies_df = spark.table("money_mop.ref_companies").toPandas()
departments_df = spark.table("money_mop.ref_departments").toPandas()
categories_df = spark.table("money_mop.ref_category_amounts").toPandas()
merchants_df = spark.table("money_mop.ref_merchants").toPandas()

# --- Generation ---
data = []
for _ in range(NUM_RECORDS):
    company = companies_df.sample(1).iloc[0]['company_name']
    department = departments_df.sample(1).iloc[0]['department_name']
    category_row = categories_df.sample(1).iloc[0]
    category = category_row['category']
    mean = category_row['mean_amount']
    std = category_row['std_amount']
    
    # Filter merchants by category
    valid_merchants = merchants_df[merchants_df['category'] == category]['merchant'].tolist()
    merchant = random.choice(valid_merchants)

    amount = round(max(1, np.random.normal(loc=mean, scale=std)), 2)

    data.append([
        str(uuid.uuid4()), 
        company, 
        department, 
        category,
        merchant, 
        amount, 
        str(YESTERDAY), 
        'transaction'
    ])

# --- Create DataFrame & Export ---
df_pd = pd.DataFrame(data, columns=[
    'transaction_id',
    'company', 
    'department', 
    'category',
    'merchant', 
    'amount', 
    'date', 
    'type'
])

df_spark = spark.createDataFrame(df_pd)
df_spark.write.format("delta").mode("append").saveAsTable("money_mop.bronze_daily_transactions")

print(f"{NUM_RECORDS} records written to table: money_mop.bronze_daily_transactions for {YESTERDAY}")
