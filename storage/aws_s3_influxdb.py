import json
import logging
import os
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_readings")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio_secret_2024")
S3_BUCKET = os.getenv("MINIO_BUCKET_RAW", "raw-data")

INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "pipeline-influx-token")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "pipeline-org")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "iot_data")


def consume_kafka_to_influx():
    """Consume Kafka sensor data and store in InfluxDB."""
    from influxdb_client import InfluxDBClient, Point, WritePrecision

    from kafka import KafkaConsumer

    influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = influx_client.write_api()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="influxdb_consumer",
    )

    logger.info("Consuming Kafka topic '%s' -> InfluxDB", KAFKA_TOPIC)
    messages_stored = 0

    for message in consumer:
        data = message.value
        device_id = data.get("device_id")
        reading_value = data.get("reading_value")
        timestamp = data.get("timestamp", int(datetime.utcnow().timestamp()))

        if device_id is None or reading_value is None:
            continue

        point = (
            Point("sensor_readings")
            .tag("device_id", str(device_id))
            .field("reading_value", float(reading_value))
            .time(timestamp, WritePrecision.S)
        )
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        messages_stored += 1

        if messages_stored % 100 == 0:
            logger.info("Stored %d readings in InfluxDB.", messages_stored)


def extract_from_influx_and_upload_s3():
    """Extract data from InfluxDB and upload to S3/MinIO."""
    import boto3
    import pandas as pd
    from influxdb_client import InfluxDBClient

    influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)

    query = f"""
    from(bucket: "{INFLUXDB_BUCKET}")
      |> range(start: -24h)
      |> filter(fn: (r) => r._measurement == "sensor_readings")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    """

    query_api = influx_client.query_api()
    tables = query_api.query(query, org=INFLUXDB_ORG)

    data = []
    for table in tables:
        for record in table.records:
            data.append(record.values)

    df = pd.DataFrame(data)
    if df.empty:
        logger.warning("No data available for extraction.")
        return

    csv_filename = f"/tmp/iot_data_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(csv_filename, index=False)
    logger.info("Extracted data saved as %s", csv_filename)

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
    s3_key = f"iot_data/{os.path.basename(csv_filename)}"
    s3.upload_file(csv_filename, S3_BUCKET, s3_key)
    logger.info("Uploaded %s to s3://%s/%s", csv_filename, S3_BUCKET, s3_key)


if __name__ == "__main__":
    consume_kafka_to_influx()
