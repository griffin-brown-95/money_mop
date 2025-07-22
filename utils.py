import logging
from pyspark.sql import SparkSession

def init_spark(app_name: str):
    logging.info(f'Starting Spark session for {app_name}')
    return SparkSession.builder.appName(app_name).getOrCreate()

def write_delta(df, table: str, mode='overwrite', partition_by=None):
    w = df.write.format('delta').mode(mode).option(mergeSchema=True)
    if partition_by:
        w.partitionBy(partition_by)
    w.saveAsTable(table)
    logging.info(f'Wrote {df.count()} records to {table}')