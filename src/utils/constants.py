from pathlib import Path

DATA_DIR = Path("data")
OUTPUT_DIR = Path("output")

CUSTOMERS_CSV = DATA_DIR / "olist_customers_dataset.csv"
ORDERS_CSV = DATA_DIR / "olist_orders_dataset.csv"
PAYMENTS_CSV = DATA_DIR / "olist_order_payments_dataset.csv"

OUTPUT_FILE = OUTPUT_DIR / "monthly_payment_stats.csv"