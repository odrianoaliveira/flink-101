FROM python:3.12-slim

RUN pip install --no-cache-dir confluent-kafka==2.5.3

COPY join_producer.py /app/join_producer.py

CMD ["python", "-u", "/app/join_producer.py"]
