import json
import glob
import psycopg2
from psycopg2.extras import execute_batch

PROCESSED_PATH = "data/processed/*.json"

DB_CONFIG = {
"host" : "localhost",
"port" : 5432,
"database" : "sales",
"user" : "sales_user",
"password" : "sales_pass"
}

CREATE_TABLE_SQL = """Create table if not exists fact_sales(
	order_id TEXT,
	order_date DATE,
	product_id TEXT,
	quantity INT,
	price NUMERIC,
	revenue NUMERIC,
	processed_at TIMESTAMP
	);
"""

INSERT_SQL = """INSERT INTO fact_sales (
    order_id, order_date, product_id, quantity, price, revenue, processed_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s);
"""

def load_orders():
	files= glob.glob(PROCESSED_PATH)
	if not files:
		raise Exception("NO processed files found")
	conn = psycopg2.connect(**DB_CONFIG)
	cursor = conn.cursor()
	cursor.execute(CREATE_TABLE_SQL)
	conn.commit()
	
	rows = []
	for file in files:
		with open(file) as f:
			data = json.load(f)
		for row in data:
			rows.append((
                		row["order_id"],
                		row["order_date"],
                		row["product_id"],
                		row["quantity"],
                		row["price"],
                		row["revenue"],
                		row["processed_at"]
            				))
	execute_batch(cursor, INSERT_SQL, rows)
	conn.commit()
	cursor.close()
	conn.close()
	print(f"loaded {len(rows)} records into fact_sales")

if __name__ == "__main__":
	load_orders()