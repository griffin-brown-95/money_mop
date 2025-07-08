from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, rand, expr

# --- Init ---
spark = SparkSession.builder.getOrCreate()

# --- Load Bronze data ---
df_bronze = spark.table("money_mop.bronze_daily_transactions")

# --- Assign experiment groups randomly ---
df_with_groups = df_bronze.withColumn("experiment_group", when(rand() < 0.5, "A").otherwise("B"))

# -- Load cashback policies ---
df_cashback = spark.table("money_mop.ref_cashback_policy")

# --- Join with cashback rates ---
df_enriched = df_with_groups.join(
    df_cashback,
    (df_with_groups.category == df_cashback.category) & 
    (df_with_groups.experiment_group == df_cashback.experiment_group) &
    (df_with_groups.date.between(df_cashback.valid_from, df_cashback.valid_to)),
    how="left"
) \
.drop(df_cashback.category) \
.drop(df_cashback.experiment_group) \
.withColumn(
    "cashback_amount", expr("amount * cashback_rate")
)

# --- Write to Silver ---
df_enriched.write.format("delta").mode("overwrite").saveAsTable("money_mop.silver_daily_transactions")

print("Data written to money_mop.silver_daily_transactions")