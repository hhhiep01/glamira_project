import csv
from pathlib import Path


INPUT_FILE = "data/interim/product_names_raw.csv"
OUTPUT_CLEAN_FILE = "data/processed/product_names_clean.csv"
OUTPUT_FAILED_FILE = "data/processed/product_names_failed.csv"


def ensure_output_dirs():
    Path(OUTPUT_CLEAN_FILE).parent.mkdir(parents=True, exist_ok=True)
    Path(OUTPUT_FAILED_FILE).parent.mkdir(parents=True, exist_ok=True)


def clean_text(value: str) -> str:
    return (value or "").strip()


def clean_product_names():
    ensure_output_dirs()

    seen_product_ids = set()
    total_rows = 0
    success_rows = 0
    clean_rows = 0
    failed_rows = 0
    duplicate_success_rows = 0
    empty_name_rows = 0

    with open(INPUT_FILE, "r", encoding="utf-8", newline="") as fin, \
         open(OUTPUT_CLEAN_FILE, "w", encoding="utf-8", newline="") as fclean, \
         open(OUTPUT_FAILED_FILE, "w", encoding="utf-8", newline="") as ffailed:

        reader = csv.DictReader(fin)

        clean_writer = csv.writer(fclean)
        failed_writer = csv.writer(ffailed)

        clean_writer.writerow([
            "product_id",
            "product_name",
        ])

        failed_writer.writerow([
            "product_id",
            "product_name",
            "url",
            "source_collection",
            "crawl_status",
        ])

        for row in reader:
            total_rows += 1

            product_id = clean_text(row.get("product_id"))
            product_name = clean_text(row.get("product_name"))
            url = clean_text(row.get("url"))
            source_collection = clean_text(row.get("source_collection"))
            crawl_status = clean_text(row.get("crawl_status"))

            if not product_id:
                failed_rows += 1
                failed_writer.writerow([
                    product_id,
                    product_name,
                    url,
                    source_collection,
                    "missing_product_id",
                ])
                continue

            if crawl_status != "success":
                failed_rows += 1
                failed_writer.writerow([
                    product_id,
                    product_name,
                    url,
                    source_collection,
                    crawl_status or "unknown_status",
                ])
                continue

            success_rows += 1

            if not product_name:
                empty_name_rows += 1
                failed_rows += 1
                failed_writer.writerow([
                    product_id,
                    product_name,
                    url,
                    source_collection,
                    "empty_product_name",
                ])
                continue

            if product_id in seen_product_ids:
                duplicate_success_rows += 1
                continue

            seen_product_ids.add(product_id)
            clean_rows += 1

            clean_writer.writerow([
                product_id,
                product_name,
            ])

    print("Done cleaning product names.")
    print(f"Input file: {INPUT_FILE}")
    print(f"Clean output: {OUTPUT_CLEAN_FILE}")
    print(f"Failed output: {OUTPUT_FAILED_FILE}")
    print(f"Total rows read: {total_rows:,}")
    print(f"Success rows found: {success_rows:,}")
    print(f"Clean unique rows written: {clean_rows:,}")
    print(f"Failed rows written: {failed_rows:,}")
    print(f"Duplicate success rows skipped: {duplicate_success_rows:,}")
    print(f"Empty product name rows: {empty_name_rows:,}")


if __name__ == "__main__":
    clean_product_names()