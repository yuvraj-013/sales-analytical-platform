FROM python:3.9-slim

WORKDIR /app

RUN pip install flask

COPY mock_orders_api.py .

CMD ["python", "mock_orders_api.py"]
