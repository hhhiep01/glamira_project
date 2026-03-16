import csv
import os
import yaml
from pymongo import MongoClient
from pathlib import Path


def load_config():
    root = Path(__file__).resolve().parents[2]
    config_path = root / "config" / "config.yaml"

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def extract_distinct_products():
    cfg = load_config()
    client = MongoClient(cfg["mongo"]["uri"])
    db = client[cfg["mongo"]["db"]]
    raw_collection = db[cfg["mongo"]["raw_collection"]]

    normal_events = [
        "view_product_detail",
        "select_product_option",
        "select_product_option_quality",
        "add_to_cart_action",
        "product_detail_recommendation_visible",
        "product_detail_recommendation_noticed",
    ]
    special_event = "product_view_all_recommend_clicked"

    os.makedirs("data/interim", exist_ok=True)
    output_file = "data/interim/distinct_products.csv"

    pipeline = [
        {
            "$match": {
                "collection": {
                    "$in": normal_events + [special_event]
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "source_collection": "$collection",
                "product_id": {
                    "$cond": [
                        {"$eq": ["$collection", special_event]},
                        "$viewing_product_id",
                        {"$ifNull": ["$product_id", "$viewing_product_id"]}
                    ]
                }
            }
        },
        {
            "$match": {
                "product_id": {"$ne": None}
            }
        },
        {
            "$project": {
                "source_collection": 1,
                "product_id": {"$toString": "$product_id"}
            }
        },
        {
            "$group": {
                "_id": "$product_id",
                "source_collection": {"$first": "$source_collection"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "product_id": "$_id",
                "source_collection": 1
            }
        }
    ]

    total_written = 0
    invalid_rows = 0
    buffer = []
    buffer_size = 1000

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "source_collection"])

        cursor = raw_collection.aggregate(pipeline, allowDiskUse=True)

        for doc in cursor:
            product_id = doc.get("product_id")
            source_collection = doc.get("source_collection")

            if product_id is not None:
                product_id = str(product_id).strip()

            if not product_id:
                invalid_rows += 1
                continue

            buffer.append([product_id, source_collection])
            total_written += 1

            if len(buffer) >= buffer_size:
                writer.writerows(buffer)
                buffer.clear()

            if total_written % 1000 == 0:
                print(f"Written: {total_written:,}")

        if buffer:
            writer.writerows(buffer)

    print("Done.")
    print(f"Output saved to {output_file}")
    print(f"Distinct product_ids written: {total_written:,}")
    print(f"Invalid rows skipped: {invalid_rows:,}")


if __name__ == "__main__":
    extract_distinct_products()