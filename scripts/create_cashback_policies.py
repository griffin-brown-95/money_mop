from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from datetime import date

# --- Define Data ---
cashback_data = [
    ("Meals", "A", 0.01, date(2025, 7, 1), date(2025, 7, 31), "test_meals_cashback"),
    ("Meals", "B", 0.05, date(2025, 7, 1), date(2025, 7, 31), "test_meals_cashback"),
    ("Travel", "A", 0.01, date(2025, 7, 1), date(2025, 7, 31), "baseline"),
    ("Travel", "B", 0.01, date(2025, 7, 1), date(2025, 7, 31), "baseline"),
    ("Entertainment", "A", 0.01, date(2025, 7, 1), date(2025, 7, 31), "baseline"),
    ("Entertainment", "B", 0.01, date(2025, 7, 1), date(2025, 7, 31), "baseline"),
    ("Misc", "A", 0.01, date(2025, 7, 1), date(2025, 7, 31), "baseline"),
    ("Misc", "B", 0.01, date(2025, 7, 1), date(2025, 7, 31), "baseline"),
    ("Supplies", "A", 0.01, date(2025, 7, 1), date(2025, 7, 31), "baseline"),
    ("Supplies", "B", 0.01, date(2025, 7, 1), date(2025, 7, 31), "baseline")
]

# --- Define Shema ---
schema = StructType([
    StructField("category", StringType(), True),
    StructField("experiment_group", StringType(), True),
    StructField("cashback_rate", DoubleType(), True),
    StructField("valid_from", DateType(), True),
    StructField("valid_to", DateType(), True),
    StructField("experiment_name", StringType(), True)
])

# --- Create Spark DataFrame ---
df_cashback = spark.createDataFrame(cashback_data, schema)
df_cashback.write.mode("overwrite").format("delta").saveAsTable("money_mop.ref_cashback_policy")  


