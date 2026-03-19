import logging
import yaml
from google.cloud import bigquery


def load_config():
    with open("config/config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def setup_logger(level="INFO"):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger(__name__)


def load_gcs_to_bigquery():
    config = load_config()
    logger = setup_logger(config["logging"]["level"])

    project_id = config["gcp"]["project_id"]
    bucket_name = config["gcp"]["bucket_name"]
    dataset_id = config["gcp"]["bq_dataset"]
    table_id = config["gcp"]["bq_table"]
    gcs_prefix = config["export"]["gcs_prefix"]

    client = bigquery.Client(project=project_id)

    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    source_uri = f"gs://{bucket_name}/{gcs_prefix}/*/*.jsonl"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    logger.info(f"Starting BigQuery load from {source_uri} to {table_ref}")
    load_job = client.load_table_from_uri(
        source_uri,
        table_ref,
        job_config=job_config
    )

    load_job.result()
    logger.info(f"Load completed successfully. Job ID: {load_job.job_id}")


if __name__ == "__main__":
    load_gcs_to_bigquery()