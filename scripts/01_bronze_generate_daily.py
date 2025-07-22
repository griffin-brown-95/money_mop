import yaml
import pandas as pd
import numpy as np
import random, uuid
from datetime import datetime, timedelta
from utils import init_spark, write_delta

def generate_daily(config):
    """
    Generate daily data for the previous day.
    This function essentially mimics the logic of the daily extract job.
    """
    spark = init_spark("bronze")
    n = random.randint(
        config["daily_records"]["min"], 
        config["daily_records"]["max"]
    )
    yesterday = pd.Timestamp.today().normalize() - timedelta(days=1)
    date_str = yesterday.date().isoformat()

    companies = spark.table(config['refs']['companies']).toPandas()
    departments = spark.table(config['refs']['departments']).toPandas()
    categories = spark.table(config['refs']['category_amounts']).toPandas()
    merchants = spark.table(config['refs']['merchants']).toPandas()

    rows = []
    for _ in range(n):
        # sample one company & department
        comp = companies.sample(1).iloc[0]["company_name"]
        dept = departments.sample(1).iloc[0]["department_name"]

        # sample one category row
        cat_row = categories.sample(1).iloc[0]
        amt = max(1, np.random.normal(cat_row["mean_amount"], cat_row["std_amount"]))

        # pick a merchant in that category
        merch_row = merchants[merchants["category"] == cat_row["category"]].sample(1).iloc[0]
        merch = merch_row["merchant"]

        rows.append({
            "transaction_id": str(uuid.uuid4()),
            "company":        comp,
            "department":     dept,
            "category":       cat_row["category"],
            "merchant":       merch,
            "amount":         round(amt, 2),
            "date":           date_str,
            "type":           "transaction"
        })

    # turn into Spark DataFrame and write out as Delta
    pdf = pd.DataFrame(rows)
    df  = spark.createDataFrame(pdf)
    write_delta(df, config["tables"]["bronze"], mode="append", partition_by="date")

if __name__ == "__main__":
    cfg = yaml.safe_load(open("config.yaml"))
    generate_daily(cfg)

