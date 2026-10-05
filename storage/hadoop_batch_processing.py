"""
Hadoop batch processing - Extract from MongoDB and upload to HDFS.

Demonstrates the MongoDB -> HDFS data flow for batch analytics.
Uses lazy imports to avoid crash at module load if dependencies
are not available.
"""

import logging
import os
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://mongodb:27017/")
MONGODB_DB = os.getenv("MONGODB_DB", "iot_data")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "sensor_readings")
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "hadoop")
HDFS_PATH = "/user/hadoop/iot_data"


def extract_from_mongodb_to_hdfs():
    """Extract batch data from MongoDB and upload to HDFS."""
    import pandas as pd
    from hdfs import InsecureClient
    from pymongo import MongoClient

    mongo_client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    db = mongo_client[MONGODB_DB]
    collection = db[MONGODB_COLLECTION]

    try:
        mongo_client.admin.command("ping")
        logger.info("Connected to MongoDB at %s", MONGODB_URI)
    except Exception as e:
        logger.error("Failed to connect to MongoDB: %s", e)
        return

    data = list(collection.find({}, {"_id": 0}))
    if not data:
        logger.warning("No data available in MongoDB for extraction.")
        return

    df = pd.DataFrame(data)
    csv_filename = f"/tmp/iot_data_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(csv_filename, index=False)
    logger.info("Extracted %d records from MongoDB to %s", len(df), csv_filename)

    try:
        hdfs_client = InsecureClient(HDFS_NAMENODE, user=HDFS_USER)
        hdfs_file_path = f"{HDFS_PATH}/{os.path.basename(csv_filename)}"
        with hdfs_client.write(hdfs_file_path, encoding="utf-8") as writer:
            df.to_csv(writer, index=False)
        logger.info("Uploaded to HDFS: %s", hdfs_file_path)
    except Exception as e:
        logger.error("HDFS upload failed: %s", e)


if __name__ == "__main__":
    extract_from_mongodb_to_hdfs()
