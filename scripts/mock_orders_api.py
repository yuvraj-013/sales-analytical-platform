from flask import Flask, jsonify, request
from datetime import date
import random

app = Flask(__name__)

products = [
	{"product_id": "P100", "category" : "Electronics", "price" : "499.99"},
	{"product_id": "P200", "category" : "Books", "price" : "19.99"},
	{"product_id": "P300", "category" : "clothing", "price" : "39.99"},
	]

@app.route("/orders")
def get_orders():
	order_date = request.args.get("date", str(date.today()))
	orders=[]

	for i in range(random.ranint(5,15)):
		product = random.choice(products)
		orders.append({
			"order_id": f"ord{random.randint(1000,9999)}",
			"order_date": order_date,
			"product_id": product["product_id"],
			"quantity": random.randint(1,5),
			"price":product["price"]
			})
	return jsonify({"orders": orders })

if __name__ == "__main__":
	app.run(port=5000)
