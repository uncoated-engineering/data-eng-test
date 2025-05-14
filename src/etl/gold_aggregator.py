
import logging
import sys
import pandas as pd
from collections import Counter


logging.basicConfig(stream=sys.stdout, level =logging.INFO)

logger = logging.getLogger(__name__)

def classify_customer_type(order_count):
    if order_count <= 2:
        return 'new'
    elif order_count <= 5:
        return 'regular'
    else:
        return 'frequent'


def payment_volume_by_zip(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(['order_month', 'customer_zip_code_prefix']).agg(
        total_payment=('adjusted_payment', 'sum'),
        num_orders=('order_id', 'count')
    ).reset_index()

def classify_customer_types(df: pd.DataFrame) -> pd.DataFrame:
    # group by customer unique id 
    customer_order_counts = df.groupby('customer_unique_id')['order_id'].nunique().reset_index()
    customer_order_counts['customer_type'] = customer_order_counts['order_id'].apply(
    lambda x: classify_customer_type(x)
    )
    logging.info(f"Done classifying customers {Counter(customer_order_counts['customer_type'])}")   
    return customer_order_counts[['customer_unique_id', 'customer_type']]


def payment_volume_by_customer_type(df: pd.DataFrame, customer_types) -> pd.DataFrame:
    df = df.merge(customer_types, on='customer_unique_id', how='left')

    df = df.groupby(['order_month', 'customer_type'], observed=True).agg(
        total_payment=('adjusted_payment', 'sum'),
        num_orders=('order_id', 'count')
    ).reset_index()

    logger.info(f"done analyzing payments by customer type {Counter(df['customer_type'])}")
    return df