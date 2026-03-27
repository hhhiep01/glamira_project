import json

import yaml
from google.cloud import bigquery, storage


def load_config():
    with open("config/config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_schema(schema_json):
    fields = []

    for item in schema_json:
        subfields = item.get("fields", [])

        if subfields:
            fields.append(
                bigquery.SchemaField(
                    item["name"],
                    item["type"],
                    mode=item.get("mode", "NULLABLE"),
                    fields=build_schema(subfields)
                )
            )
        else:
            fields.append(
                bigquery.SchemaField(
                    item["name"],
                    item["type"],
                    mode=item.get("mode", "NULLABLE")
                )
            )

    return fields


def list_jsonl_uris(project_id, bucket_name, prefix):
    storage_client = storage.Client(project=project_id)
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)

    uris = []
    for blob in blobs:
        if blob.name.endswith(".jsonl"):
            uris.append(f"gs://{bucket_name}/{blob.name}")

    return sorted(uris)


def load_raw_events(client, project_id, dataset_id, schema, raw_uris):
    table_id = f"{project_id}.{dataset_id}.raw_events"

    table = bigquery.Table(table_id, schema=schema)
    client.delete_table(table_id, not_found_ok=True)
    client.create_table(table, exists_ok=True)

    print(f"Raw table ready: {table_id}")
    print(f"Found {len(raw_uris)} raw file(s)")

    if not raw_uris:
        print("No raw files found")
        return

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ignore_unknown_values=False,
        max_bad_records=0,
    )

    job = client.load_table_from_uri(raw_uris, table_id, job_config=job_config)
    job.result()

    print(f"Loaded -> {table_id}")


def load_lookup_table(client, table_id, uris):
    if not uris:
        print(f"No files for {table_id}")
        return

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    job = client.load_table_from_uri(uris, table_id, job_config=job_config)
    job.result()

    print(f"Loaded -> {table_id}")


def main():
    config = load_config()

    project_id = config["gcp"]["project_id"]
    dataset_id = config["gcp"]["bq_dataset"]
    bucket_name = config["gcp"]["bucket_name"]
    location = config["gcp"].get("location", "US")

    client = bigquery.Client(project=project_id)

    dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset.location = location
    client.create_dataset(dataset, exists_ok=True)

    print("Dataset ready")

    with open("schemas/raw_events_schema.json", "r", encoding="utf-8") as f:
        schema_json = json.load(f)

    raw_schema = build_schema(schema_json)
    raw_uris = list_jsonl_uris(project_id, bucket_name, "raw/raw_data/")

    load_raw_events(
        client=client,
        project_id=project_id,
        dataset_id=dataset_id,
        schema=raw_schema,
        raw_uris=raw_uris
    )

    ip_uris = list_jsonl_uris(project_id, bucket_name, "raw/ip_locations/")
    load_lookup_table(
        client,
        f"{project_id}.{dataset_id}.ip_locations_raw",
        ip_uris
    )

    product_uris = list_jsonl_uris(project_id, bucket_name, "raw/product_names/")
    load_lookup_table(
        client,
        f"{project_id}.{dataset_id}.product_names_raw",
        product_uris
    )


if __name__ == "__main__":
    main()