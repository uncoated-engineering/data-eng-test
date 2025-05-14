import pandas as pd
from etl.gold_aggregator import payment_volume_by_zip  # Adjust the import based on your structure

def test_payment_volume_by_zip():
    # Sample input DataFrame
    data = {
        'order_month': ['2024-01', '2024-01', '2024-01', '2024-02'],
        'customer_zip_code_prefix': [12345, 12345, 67890, 12345],
        'adjusted_payment': [100.0, 200.0, 300.0, 150.0],
        'order_id': ['a1', 'a2', 'a3', 'a4']
    }
    df = pd.DataFrame(data)

    # Expected output DataFrame
    expected_data = {
        'order_month': ['2024-01', '2024-01', '2024-02'],
        'customer_zip_code_prefix': [12345, 67890, 12345],
        'total_payment': [300.0, 300.0, 150.0],
        'num_orders': [2, 1, 1]
    }
    expected_df = pd.DataFrame(expected_data)

    # Run the function
    result_df = payment_volume_by_zip(df)

    # Sort both DataFrames for comparison (optional but good practice)
    result_df = result_df.sort_values(by=['order_month', 'customer_zip_code_prefix']).reset_index(drop=True)
    expected_df = expected_df.sort_values(by=['order_month', 'customer_zip_code_prefix']).reset_index(drop=True)

    # Compare
    pd.testing.assert_frame_equal(result_df, expected_df)