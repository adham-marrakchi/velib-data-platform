FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python==2.0.2

COPY producer.py .

CMD ["python", "producer.py"]
