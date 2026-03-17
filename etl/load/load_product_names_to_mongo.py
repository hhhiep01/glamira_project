import csv
from pathlib import Path

from pymongo import MongoClient, UpdateOne
import yaml


INPUT_FILE = "data/processed/product_names_clean.csv"


def load_config():
    root = Path(__file__).resolve().parents[2]
    config_path = root / "config" / "config.yaml"

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def clean_text(value: str) -> str:
    return (value or "").strip()


def load_product_names_to_mongo():
    cfg = load_config()

    client = MongoClient(cfg["mongo"]["uri"])
    db = client[cfg["mongo"]["db"]]

    collection = db["product_names"]

    collection.create_index("product_id", unique=True)

    total_rows = 0
    valid_rows = 0
    skipped_rows = 0
    operations = []

    with open(INPUT_FILE, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            total_rows += 1

            product_id = clean_text(row.get("product_id"))
            product_name = clean_text(row.get("product_name"))

            if not product_id or not product_name:
                skipped_rows += 1
                continue

            operations.append(
                UpdateOne(
                    {"product_id": product_id},
                    {
                        "$set": {
                            "product_id": product_id,
                            "product_name": product_name,
                        }
                    },
                    upsert=True,
                )
            )
            valid_rows += 1

    if operations:
        result = collection.bulk_write(operations, ordered=False)
        print("Bulk write completed.")
        print(f"Inserted: {result.upserted_count:,}")
        print(f"Modified: {result.modified_count:,}")
        print(f"Matched: {result.matched_count:,}")
    else:
        print("No valid rows to write.")

    print(f"Input file: {INPUT_FILE}")
    print(f"Total rows read: {total_rows:,}")
    print(f"Valid rows: {valid_rows:,}")
    print(f"Skipped rows: {skipped_rows:,}")
    print(f"Target collection: product_names")


if __name__ == "__main__":
    load_product_names_to_mongo()