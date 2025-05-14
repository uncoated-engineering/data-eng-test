import pandas as pd
from pathlib import Path
from utils.logger import get_logger

logger = get_logger(__name__)

def _get_bronze_file_path(output_dir: str, table_name: str, file_name: str) -> Path:
    return Path(output_dir) / "bronze" / table_name / f"{file_name}.parquet"


def load_csvs(data_dir: str):
    data = {}
    file_info = {}
    for csv_file in Path(data_dir).glob("*.csv"):
        name = csv_file.stem.replace("olist_", "").replace("_dataset", "")
        df = pd.read_csv(csv_file)
        data[name] = df
        file_info[name] = csv_file.name
    return data, file_info


def save_bronze(raw_data: dict, output_dir: str, file_info: dict):
    for table_name, df in raw_data.items():
        file_name = Path(file_info[table_name]).stem
        save_path = _get_bronze_file_path(output_dir, table_name, file_name)

        if save_path.exists():
            logger.info(f"Skipping already ingested {file_name} for {table_name}")
            continue

        save_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(save_path, index=False)
        logger.info(f"Saved bronze for {table_name} ({file_name})")
