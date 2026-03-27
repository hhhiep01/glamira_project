import json
import logging
from collections import defaultdict
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


def get_existing_file_index(storage_client, bucket_name, prefix):
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    max_index = -1

    for blob in blobs:
        name = blob.name
        if "part-" in name:
            try:
                idx = int(name.split("part-")[1].split(".")[0])
                max_index = max(max_index, idx)
            except Exception:
                pass

    return max_index + 1


def upload_jsonl(storage_client, bucket_name, blob_name, batch, logger):
    content = "\n".join(
        json.dumps(x, ensure_ascii=False, default=str) for x in batch
    )

    storage_client.bucket(bucket_name).blob(blob_name).upload_from_string(
        content,
        content_type="application/json"
    )

    logger.info(f"Uploaded {len(batch)} records to gs://{bucket_name}/{blob_name}")


def export_normal_collection(
    collection,
    mongo_collection,
    storage_client,
    bucket_name,
    gcs_prefix,
    today,
    batch_size,
    logger,
    skip_docs=0
):
    prefix = f"{gcs_prefix}/{mongo_collection}/{today}/"
    file_index = get_existing_file_index(storage_client, bucket_name, prefix)

    if skip_docs == 0:
        skip_docs = file_index * batch_size

    logger.info(f"{mongo_collection}: resume from file_index={file_index}")
    logger.info(f"{mongo_collection}: skipping {skip_docs} documents")

    cursor = collection.find({}, no_cursor_timeout=True).skip(skip_docs).batch_size(batch_size)

    batch = []
    total_exported = 0

    try:
        for i, doc in enumerate(cursor, start=1):
            batch.append(convert_doc(doc))

            if i % 10000 == 0:
                logger.info(f"{mongo_collection}: processed {i + skip_docs} docs")

            if len(batch) >= batch_size:
                blob_name = f"{prefix}part-{file_index:04d}.jsonl"
                upload_jsonl(storage_client, bucket_name, blob_name, batch, logger)

                total_exported += len(batch)
                batch = []
                file_index += 1

        if batch:
            blob_name = f"{prefix}part-{file_index:04d}.jsonl"
            upload_jsonl(storage_client, bucket_name, blob_name, batch, logger)
            total_exported += len(batch)

        logger.info(f"{mongo_collection}: export completed. Total exported: {total_exported}")

    finally:
        cursor.close()


def export_raw_data_by_event(
    collection,
    storage_client,
    bucket_name,
    gcs_prefix,
    today,
    batch_size,
    logger
):
    logger.info("raw_data: exporting by inner field 'collection'")

    cursor = collection.find({}, no_cursor_timeout=True).batch_size(batch_size)

    batches = defaultdict(list)
    file_indexes = {}
    total_exported = defaultdict(int)

    try:
        for i, doc in enumerate(cursor, start=1):
            converted = convert_doc(doc)
            event_name = converted.get("collection", "unknown")

            event_name = str(event_name).strip().replace("/", "_").replace(" ", "_")

            if event_name not in file_indexes:
                prefix = f"{gcs_prefix}/raw_data/{event_name}/{today}/"
                file_indexes[event_name] = get_existing_file_index(
                    storage_client,
                    bucket_name,
                    prefix
                )

            batches[event_name].append(converted)

            if i % 10000 == 0:
                logger.info(f"raw_data: processed {i} docs")

            if len(batches[event_name]) >= batch_size:
                prefix = f"{gcs_prefix}/raw_data/{event_name}/{today}/"
                blob_name = f"{prefix}part-{file_indexes[event_name]:04d}.jsonl"

                upload_jsonl(
                    storage_client,
                    bucket_name,
                    blob_name,
                    batches[event_name],
                    logger
                )

                total_exported[event_name] += len(batches[event_name])
                batches[event_name] = []
                file_indexes[event_name] += 1

        for event_name, batch in batches.items():
            if batch:
                prefix = f"{gcs_prefix}/raw_data/{event_name}/{today}/"
                blob_name = f"{prefix}part-{file_indexes[event_name]:04d}.jsonl"

                upload_jsonl(
                    storage_client,
                    bucket_name,
                    blob_name,
                    batch,
                    logger
                )

                total_exported[event_name] += len(batch)

        logger.info("raw_data: export completed by event type")
        for event_name, total in total_exported.items():
            logger.info(f"raw_data/{event_name}: total exported = {total}")

    finally:
        cursor.close()


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

        today = datetime.now().strftime("%Y-%m-%d")

        for collection_key in source_collection_keys:
            mongo_collection = config["mongo"][collection_key]
            collection = mongo_client[mongo_db][mongo_collection]

            logger.info(f"Start exporting collection: {mongo_collection}")

            if collection_key == "raw_collection":
                logger.info("Using event-based export for raw_data")
                export_raw_data_by_event(
                    collection=collection,
                    storage_client=storage_client,
                    bucket_name=bucket_name,
                    gcs_prefix=gcs_prefix,
                    today=today,
                    batch_size=batch_size,
                    logger=logger
                )
            else:
                logger.info(f"Using normal export for {mongo_collection}")
                export_normal_collection(
                    collection=collection,
                    mongo_collection=mongo_collection,
                    storage_client=storage_client,
                    bucket_name=bucket_name,
                    gcs_prefix=gcs_prefix,
                    today=today,
                    batch_size=batch_size,
                    logger=logger
                )

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