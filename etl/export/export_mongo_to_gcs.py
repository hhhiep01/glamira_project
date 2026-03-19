import json
import logging
from datetime import datetime

import yaml
from bson import ObjectId
from pymongo import MongoClient
from google.cloud import storage


def load_config():
    with open("config/config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def setup_logger(level="INFO"):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger(__name__)


def convert_doc(doc):
    new_doc = {}
    for key, value in doc.items():
        if isinstance(value, ObjectId):
            new_doc[key] = str(value)
        else:
            new_doc[key] = value
    return new_doc


def export_to_gcs():
    config = load_config()
    logger = setup_logger(config["logging"]["level"])

    mongo_uri = config["mongo"]["uri"]
    mongo_db = config["mongo"]["db"]
    source_collection_keys = config["export"]["source_collection_keys"]

    project_id = config["gcp"]["project_id"]
    bucket_name = config["gcp"]["bucket_name"]

    batch_size = config["export"]["batch_size"]
    gcs_prefix = config["export"]["gcs_prefix"]

    mongo_client = None

    try:
        logger.info("Connecting to MongoDB")
        mongo_client = MongoClient(mongo_uri)

        logger.info("Connecting to Google Cloud Storage")
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)

        today = datetime.now().strftime("%Y-%m-%d")

        for collection_key in source_collection_keys:
            mongo_collection = config["mongo"][collection_key]
            collection = mongo_client[mongo_db][mongo_collection]

            total_docs = collection.count_documents({})
            logger.info(f"Start exporting collection: {mongo_collection}")
            logger.info(f"Total documents: {total_docs}")

            cursor = collection.find({}, no_cursor_timeout=True).batch_size(batch_size)

            batch = []
            file_index = 0
            total_exported = 0

            try:
                for doc in cursor:
                    batch.append(convert_doc(doc))

                    if len(batch) >= batch_size:
                        blob_name = f"{gcs_prefix}/{mongo_collection}/{today}/part-{file_index:04d}.jsonl"
                        content = "\n".join(
                            json.dumps(x, ensure_ascii=False, default=str) for x in batch
                        )

                        bucket.blob(blob_name).upload_from_string(
                            content,
                            content_type="application/json"
                        )

                        logger.info(
                            f"Uploaded {len(batch)} records to gs://{bucket_name}/{blob_name}"
                        )

                        total_exported += len(batch)
                        batch = []
                        file_index += 1

                if batch:
                    blob_name = f"{gcs_prefix}/{mongo_collection}/{today}/part-{file_index:04d}.jsonl"
                    content = "\n".join(
                        json.dumps(x, ensure_ascii=False, default=str) for x in batch
                    )

                    bucket.blob(blob_name).upload_from_string(
                        content,
                        content_type="application/json"
                    )

                    logger.info(
                        f"Uploaded {len(batch)} records to gs://{bucket_name}/{blob_name}"
                    )
                    total_exported += len(batch)

                logger.info(
                    f"Export completed for collection {mongo_collection}. Total exported records: {total_exported}"
                )

            finally:
                cursor.close()

        logger.info("All collections exported successfully")

    except Exception as e:
        logger.exception(f"Export failed: {e}")
        raise

    finally:
        if mongo_client:
            mongo_client.close()
        logger.info("Closed MongoDB connection")


if __name__ == "__main__":
    export_to_gcs()