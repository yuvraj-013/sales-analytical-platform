import json
import glob
import os
from datetime import datetime

RAW_DATA_PATH = "/opt/airflow/data/raw/orders/*.json"
PROCESSED_DIR = "/opt/airflow/data/processed/orders"


def transform_orders():
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    processed_orders = []

    raw_files = glob.glob(RAW_DATA_PATH)

    if not raw_files:
        raise Exception("No raw files found for transformation")

    for file in raw_files:
        with open(file) as f:
            data = json.load(f)

        for order in data["orders"]:
            processed_orders.append({
                "order_id": order["order_id"],
                "order_date": order["order_date"],
                "product_id": order["product_id"],
                "quantity": int(order["quantity"]),
                "price": float(order["price"]),
                "revenue": int(order["quantity"]) * float(order["price"]),
                "processed_at": datetime.utcnow().isoformat()
            })

    output_file = f"processed_orders_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    output_path = os.path.join(PROCESSED_DIR, output_file)

    with open(output_path, "w") as f:
        json.dump(processed_orders, f)

    print(f"Transformed data written to {output_path}")


if __name__ == "__main__":
    transform_orders()
