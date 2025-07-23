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

    # src/utils.py

from scipy.stats import norm
from pyspark.sql import Row, SparkSession
import yaml

def generate_sequential_plan(power_df, n_looks: int, alpha: float, method: str = "pocock", experiment_name: str = None):
    """
    Build a sequential testing plan table.
    
    Args:
      power_df         Spark DataFrame with columns:
                          - category (STRING)
                          - n_per_group (INT)
      n_looks          Number of interim looks/checkpoints (K)
      alpha            Overall Type-I error (e.g. 0.05)
      method           'pocock' or 'obrien_fleming'
      experiment_name  Optional string tag for each row
    
    Returns:
      Spark DataFrame with columns:
        category,
        checkpoint_num,
        transactions_req,
        information_frac,
        efficacy_z,
        futility_z,
        harm_z,
        [experiment_name]
    """
    rows = []
    for r in power_df.collect():
        cat = r["category"]
        N   = r["n_per_group"]
        for i in range(1, n_looks + 1):
            info_frac = i / n_looks
            if method.lower() == "pocock":
                alpha_adj = 1 - (1 - alpha) ** (1 / n_looks)
                z_bound   = norm.ppf(1 - alpha_adj / 2)
            elif method.lower() == "obrien_fleming":
                z_bound   = norm.ppf(1 - (alpha / 2) / (info_frac ** 0.5))
            else:
                raise ValueError(f"Unknown method: {method}")

            row = {
                "category":         cat,
                "checkpoint_num":   i,
                "transactions_req": int(N * info_frac),
                "information_frac": float(info_frac),
                "efficacy_z":       float(z_bound),
                "futility_z":       float(-z_bound),
                "harm_z":           float(-z_bound),
            }
            if experiment_name is not None:
                row["experiment_name"] = experiment_name

            rows.append(Row(**row))

    spark = SparkSession.builder.getOrCreate()
    return spark.createDataFrame(rows)
