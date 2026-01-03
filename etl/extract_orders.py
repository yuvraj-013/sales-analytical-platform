import requests
import json
import os
from datetime import date, datetime

API_URL = "http://mock-orders-api:5000/orders"
RAW_DATA_DIR = "/opt/airflow/data/raw/orders"

def extract_orders(extract_date: str):
	response = requests.get(API_URL, params={"date": extract_date})
	response.raise_for_status()
	orders_data = response.json()
	os.makedirs(RAW_DATA_DIR, exist_ok=True)
	filename = f"orders_{extract_date}_{datetime.utcnow().strftime('%H%M%S')}.json"
	file_path = os.path.join(RAW_DATA_DIR, filename)
	with open(file_path, "w") as f:
		json.dump(orders_data,f)
	print(f"Raw orders saved to {file_path}")

if __name__ == "__main__":
    extract_date = str(date.today())
    extract_orders(extract_date)