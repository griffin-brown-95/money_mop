from pyspark.sql.functions import sum as _sum, avg, count, col, round
from utils import init_spark, write_delta
import yaml

def aggregate_gold(config):
    spark = init_spark("gold")
    df = spark.table(config["tables"]["silver"])

    summary = (
        df.groupBy("category", "experiment_group", "experiment_name")
            .agg(
                count("transaction_id").alias("transactions"),
                round(_sum("amount"), 2).alias("total_spend"),
                round(avg("amount"), 2).alias("avg_spend"),
                round(_sum("cashback_amount"), 2).alias("total_cashback"),
                round(avg("cashback_amount"), 2).alias("avg_cashback")
            )
        .orderBy("category", "experiment_group")
    )

    #write_delta(summary, config["tables"]["gold"])

    summary.show()

if __name__ == "__main__":
    cfg = yaml.safe_load(open("../config.yaml"))
    aggregate_gold(cfg)