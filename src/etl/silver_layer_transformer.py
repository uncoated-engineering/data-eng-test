import pandas as pd
from utils.logger import get_logger

logger = get_logger(__name__)



def adjust_payment(df):
    return df.apply(
        lambda row: row["payment_value"] * 0.99 if row["payment_type"] == "credit_card" else row["payment_value"],
        axis=1
    )


def build_silver_layer(customers_df, orders_df, payments_df) -> pd.DataFrame:
    # Merge payments with orders
    merged_orders_payments = payments_df.merge(orders_df, on='order_id', how='inner')    
    merged = merged_orders_payments.merge(customers_df, on='customer_id', how='inner') # group later by customer_unique_id

    logger.info(f"merged all dataset together with shape {merged.shape}")

    # Clean/augment fields
    merged['order_purchase_timestamp'] = pd.to_datetime(merged['order_purchase_timestamp'])
    merged["order_month"] = merged["order_purchase_timestamp"].dt.to_period("M").astype(str)
    
    # Add adjusted payment
    merged['adjusted_payment'] = adjust_payment(merged)

    logger.info(f"silver layer ready with shape {merged.shape}")

    return merged
