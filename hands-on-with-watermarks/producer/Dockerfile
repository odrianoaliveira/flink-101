FROM python:3.12-slim

RUN pip install --no-cache-dir confluent-kafka==2.5.3

COPY click_producer.py /app/click_producer.py

CMD ["python", "/app/click_producer.py"]
