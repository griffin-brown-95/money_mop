from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, avg, count, col, round

# --- Init Spark ---
spark = SparkSession.builder.getOrCreate()

# --- Load Silver Data ---
df_silver = spark.table("money_mop.silver_daily_transactions")

# --- Aggregated experiment metrics ---
df_gold = df_silver.groupBy("category", "experiment_group", "experiment_name").agg(
    count("transaction_id").alias("transactions"),
    round(_sum("amount"), 2).alias("total_spend"),
    round(avg("amount"), 2).alias("avg_spend"),
    round(_sum("cashback_amount"), 2).alias("total_cashback"),
    round(avg("cashback_amount"), 2).alias("avg_cashback")
).orderBy("category", "experiment_group")


# --- Write to gold ---
df_gold.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .format("delta") \
    .saveAsTable("money_mop.gold_experiment_metrics")

print("Gold experiement layer written to money_mop.gold_experiment_metrics")