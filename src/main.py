# helloasso_etl/main.py

from etl.bronze_layer import load_csvs, save_bronze
from etl.silver_layer_transformer import build_silver_layer
from etl.gold_aggregator import (
    payment_volume_by_zip,
    payment_volume_by_customer_type,
    classify_customer_types
)
from pathlib import Path
from utils.logger import get_logger

try:
    profile
except NameError:
    def profile(func):
        return func

logger = get_logger(__name__)

@profile
def run_pipeline(data_dir: str = "dataset", output_dir: str = "output"):
    
    # Bronze: Load raw CSVs
    logger.info("Loading raw data...")
    raw_data, file_info = load_csvs(data_dir)
    save_bronze(raw_data, output_dir, file_info)

    # Silver: Clean + Join
    logger.info("Building silver dataset...")
    silver_df = build_silver_layer(
        raw_data["customers"],
        raw_data["orders"],
        raw_data["order_payments"]
    )
    silver_path = Path(output_dir) / "silver"
    silver_path.mkdir(parents=True, exist_ok=True)
    for month, group in silver_df.groupby("order_month"):
        group.to_parquet(silver_path / f"merged_orders_{month}.parquet", index=False)

    # Gold: Aggregations
    logger.info("Computing gold indicators...")
    gold_path = Path(output_dir) / "gold"
    gold_path.mkdir(parents=True, exist_ok=True)

    customer_types = classify_customer_types(silver_df)
    customer_types.to_csv(gold_path/ f"customer_types.csv", index=False)

    payment_by_zip = payment_volume_by_zip(silver_df)
    payment_by_zip.to_csv(gold_path / "payment_by_zip.csv", index=False)

    payment_by_customer_type = payment_volume_by_customer_type(silver_df, customer_types)
    payment_by_customer_type.to_csv(gold_path / "payment_by_customer_type.csv", index=False)

    logger.info("Pipeline completed.")

if __name__ == "__main__":
    run_pipeline()
