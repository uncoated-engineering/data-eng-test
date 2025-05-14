import tempfile
from pathlib import Path
from unittest.mock import patch
import pandas as pd
from etl.bronze_layer import load_csvs, save_bronze

def test_load_csvs(tmp_path):
    # Setup: create fake CSV file
    csv_path = tmp_path / "olist_customers_dataset.csv"
    df = pd.DataFrame({"id": [1], "name": ["Alice"]})
    df.to_csv(csv_path, index=False)

    # Run
    data, file_info = load_csvs(tmp_path)

    # Assert
    assert "customers" in data
    assert data["customers"].equals(df)
    assert file_info["customers"] == "olist_customers_dataset.csv"




def test_save_bronze(tmp_path):
    # Setup: mock raw data and file info
    df = pd.DataFrame({"id": [1]})
    raw_data = {"orders": df}
    file_info = {"orders": "olist_orders_dataset.csv"}

    with patch("etl.bronze_layer._get_bronze_file_path") as mock_path:
        mock_file = tmp_path / "orders.parquet"
        mock_path.return_value = mock_file

        save_bronze(raw_data, tmp_path, file_info)

        assert mock_file.exists()
        result_df = pd.read_parquet(mock_file)
        assert result_df.equals(df)