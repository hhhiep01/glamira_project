import csv
from pathlib import Path

INPUT_FILE = "data/interim/product_sources.csv"
OUTPUT_FILE = "data/interim/product_best_url.csv"

EVENT_PRIORITY = {
    "view_product_detail": 1,
    "select_product_option": 2,
    "select_product_option_quality": 3,
    "add_to_cart_action": 4,
    "product_detail_recommendation_visible": 5,
    "product_detail_recommendation_noticed": 6,
    "product_view_all_recommend_clicked": 7,
}

def select_best_product_url():
    best_urls = {}
    total_rows = 0
    with open(INPUT_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:

            total_rows += 1

            product_id = row['product_id']
            url = row['url']
            event = row['source_collection']
            priority = EVENT_PRIORITY.get(event, 999)

            if product_id not in best_urls :
                best_urls[product_id] = {
                    "url": url,
                    "priority": priority, 
                    "event": event,
                }

            else:
                if priority < best_urls[product_id]["priority"]:
                    best_urls[product_id] = {
                        "url": url,
                        "priority": priority,
                        "event": event,
                    }

            if total_rows % 500000 == 0:
                print(
                    f"Processed: {total_rows:,} rows | "
                    f"Unique products: {len(best_urls):,}"
                )
    Path("data/interim").mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "url", "source_collection"])
        for pid, data in best_urls.items():
            writer.writerow([pid, data["url"], data["event"]])

    print("Done")
    print("Total rows scanned:", total_rows)
    print("Distinct product_id:", len(best_urls))
    print("Output:", OUTPUT_FILE)

if __name__ == "__main__":
    select_best_product_url()