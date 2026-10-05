FROM python:3.12-slim

RUN pip install --no-cache-dir confluent-kafka==2.5.3

COPY order_producer.py /app/order_producer.py

CMD ["python", "/app/order_producer.py"]
