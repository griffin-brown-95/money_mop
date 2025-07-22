from pyspark.sql.functions import col, when, rand, expr
from utils import init_spark, write_delta
import yaml

def enrich_silver(config):
    spark = init_spark(config)
    bronze = spark.table(config["tables"]["bronze"])
    policy = spark.table(config["tables"]["policy"])

    enriched = (
        bronze
        .withColumn("experiment_group", when(rand() < 0.5, "A").otherwise("B"))
        .join(policy, ["category", "experiment_group"], "left")
        .withColumn("cashback_amount", expr("amount * cashback_rate"))
    )
    
    write_delta(enriched, config["tables"]["silver"], partition_by="date")

if __name__=="__main__":
    cfg = yaml.safe_load(open("config.yaml"))
    enrich_silver(cfg)

