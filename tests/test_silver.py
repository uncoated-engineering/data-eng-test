import pandas as pd
from pandas.testing import assert_frame_equal
from etl.silver_layer_transformer import adjust_payment, build_silver_layer  # update path accordingly

def test_adjust_payment():
    df = pd.DataFrame({
        "payment_type": ["credit_card", "boleto"],
        "payment_value": [100.0, 200.0]
    })
    expected = pd.Series([99.0, 200.0])
    result = adjust_payment(df)
    pd.testing.assert_series_equal(result, expected, check_names=False)

def test_build_silver_layer():
    customers_df = pd.DataFrame({
        "customer_id": ["c1"],
        "customer_unique_id": ["u1"],
        "customer_zip_code_prefix": [12345]
    })

    orders_df = pd.DataFrame({
        "order_id": ["o1"],
        "customer_id": ["c1"],
        "order_purchase_timestamp": ["2024-01-15 12:00:00"]
    })

    payments_df = pd.DataFrame({
        "order_id": ["o1"],
        "payment_type": ["credit_card"],
        "payment_value": [100.0]
    })

    result_df = build_silver_layer(customers_df, orders_df, payments_df)

    expected_df = pd.DataFrame({
        "order_id": ["o1"],
        "payment_type": ["credit_card"],
        "payment_value": [100.0],
        "customer_id": ["c1"],
        "order_purchase_timestamp": [pd.to_datetime("2024-01-15 12:00:00")],
        "customer_unique_id": ["u1"],
        "customer_zip_code_prefix": [12345],
        "order_month": ["2024-01"],
        "adjusted_payment": [99.0]
    })

    # Use subset of columns in correct order
    result_df = result_df[expected_df.columns]

    assert_frame_equal(result_df, expected_df)
