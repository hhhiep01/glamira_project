import json
from collections import defaultdict
import os

import yaml
from google.cloud import storage


def load_config():
    with open("config/config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def detect_type(value):
    if value is None:
        return "STRING"

    if isinstance(value, bool):
        return "BOOLEAN"

    if isinstance(value, int) and not isinstance(value, bool):
        return "INT64"

    if isinstance(value, float):
        return "FLOAT64"
    return "STRING"


def merge_type(old_type, new_type):
    if old_type == new_type:
        return old_type

    if {old_type, new_type} == {"INT64", "FLOAT64"}:
        return "FLOAT64"

    return "STRING"


def get_event_name_from_blob(blob_name):
    parts = blob_name.split("/")
    if len(parts) >= 4:
        return parts[2]
    return None


def infer_schema_from_json_line(line, schema_map, skip_fields):
    try:
        doc = json.loads(line)
    except Exception:
        return

    if not isinstance(doc, dict):
        return

    for key, value in doc.items():
        if key in skip_fields:
            continue

        new_type = detect_type(value)

        if key not in schema_map:
            schema_map[key] = new_type
        else:
            schema_map[key] = merge_type(schema_map[key], new_type)


def manual_nested_fields():
    return [
        {
            "name": "option",
            "type": "JSON",
            "mode": "NULLABLE",
        },
        {
            "name": "cart_products",
            "type": "JSON",
            "mode": "NULLABLE",
        },
    ]


def main():
    config = load_config()

    project_id = config["gcp"]["project_id"]
    bucket_name = config["gcp"]["bucket_name"]

    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)

    raw_prefix = "raw/raw_data/"
    max_files_per_event = 3
    max_lines_per_file = 3000

    skip_fields = {"option", "cart_products"}

    blobs = storage_client.list_blobs(bucket_name, prefix=raw_prefix)

    event_files = defaultdict(list)
    for blob in blobs:
        if blob.name.endswith(".jsonl"):
            event_name = get_event_name_from_blob(blob.name)
            if event_name:
                event_files[event_name].append(blob.name)

    print(f"Found {len(event_files)} event folder(s)")

    union_schema = {}

    for event_name, files in sorted(event_files.items()):
        print(f"Scanning event: {event_name}")

        event_schema = {}
        selected_files = sorted(files)[:max_files_per_event]

        for blob_name in selected_files:
            print(f"  Reading gs://{bucket_name}/{blob_name}")
            content = bucket.blob(blob_name).download_as_text(encoding="utf-8")

            line_count = 0
            for line in content.splitlines():
                if not line.strip():
                    continue

                infer_schema_from_json_line(line, event_schema, skip_fields)
                line_count += 1

                if line_count >= max_lines_per_file:
                    break

        for key, dtype in event_schema.items():
            if key not in union_schema:
                union_schema[key] = dtype
            else:
                union_schema[key] = merge_type(union_schema[key], dtype)


    safe_string_fields = {
        "_id",
        "order_id",
        "cat_id",
        "viewing_product_id",
        "store_id",
        "user_id_db",
        "show_recommendation",
        "api_version",
        "local_time",
        "email_address",
        "device_id",
        "resolution",
        "ip",
        "current_url",
        "referrer_url",
        "collection",
        "user_agent",
    }

    for field_name in safe_string_fields:
        union_schema[field_name] = "STRING"

    if "time_stamp" not in union_schema:
        union_schema["time_stamp"] = "INT64"

    schema = []
    for key in sorted(union_schema.keys()):
        schema.append({
            "name": key,
            "type": union_schema[key],
            "mode": "NULLABLE"
        })

    schema.extend(manual_nested_fields())
    os.makedirs("schemas", exist_ok=True)
    with open("schemas/raw_events_schema.json", "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2, ensure_ascii=False)

    print("Saved: schemas/raw_events_schema.json")


if __name__ == "__main__":
    main()