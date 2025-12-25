import json
import glob
from datetime import datetime, date

RAW_DATA_PATH = "data/raw/*.json"

REQUIRED_FIELDS = [
    "order_id",
    "order_date",
    "product_id",
    "quantity",
    "price"
]


def validate_order(order: dict) -> bool:
    # Check required fields
    for field in REQUIRED_FIELDS:
        if field not in order:
            print(f"Invalid order: missing field {field} -> {order}")
            return False

    # Validate date
    try:
        date.fromisoformat(order["order_date"])
    except Exception:
        print(f"Invalid date format -> {order['order_date']}")
        return False

    # Validate quantity
    if not isinstance(order["quantity"], int) or order["quantity"] <= 0:
        print(f"Invalid quantity -> {order['quantity']}")
        return False

    # Validate price
    if not isinstance(order["price"], (int, float)) or order["price"] <= 0:
        print(f"Invalid price -> {order['price']}")
        return False

    return True


def validate_files():
    files = glob.glob(RAW_DATA_PATH)

    if not files:
        raise Exception("No raw order files found")

    total_orders = 0
    invalid_orders = 0

    for file in files:
        with open(file) as f:
            data = json.load(f)

        orders = data.get("orders", [])

        if not orders:
            raise Exception(f"No orders in file {file}")

        for order in orders:
            total_orders += 1
            if not validate_order(order):
                invalid_orders += 1

    if invalid_orders > 0:
        raise Exception(
            f"Validation failed: {invalid_orders} invalid orders found"
        )

    print(f"Validation successful: {total_orders} orders validated")


if __name__ == "__main__":
    validate_files()
