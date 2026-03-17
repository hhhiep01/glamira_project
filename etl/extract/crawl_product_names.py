import csv
import random
import time
from pathlib import Path
from typing import Dict, Set

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


INPUT_FILE = "data/interim/distinct_products.csv"
OUTPUT_FILE = "data/interim/product_names_raw.csv"

BASE_URL = "https://www.glamira.com/catalog/product/view/id/"

BATCH_SIZE = 50
BATCH_SLEEP_SECONDS = 6
MIN_REQUEST_SLEEP_SECONDS = 0.6
MAX_REQUEST_SLEEP_SECONDS = 1.4
PAGE_TIMEOUT_MS = 12000
MAX_RETRIES = 2
LIMIT_ROWS = None


def build_canonical_url(product_id: str) -> str:
    return f"{BASE_URL}{product_id}"


def load_input_rows() -> list[dict]:
    rows = []

    with open(INPUT_FILE, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            product_id = (row.get("product_id") or "").strip()
            source_collection = (row.get("source_collection") or "").strip()

            if not product_id:
                continue

            rows.append({
                "product_id": product_id,
                "source_collection": source_collection,
            })

            if LIMIT_ROWS is not None and len(rows) >= LIMIT_ROWS:
                break

    return rows


def load_processed_ids() -> Set[str]:
    output_path = Path(OUTPUT_FILE)

    if not output_path.exists():
        return set()

    processed_ids = set()

    with open(output_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            product_id = (row.get("product_id") or "").strip()
            if product_id:
                processed_ids.add(product_id)

    return processed_ids


def extract_product_name_from_react(page) -> str:
    try:
        name = page.evaluate("""
        () => {
            if (typeof react_data !== "undefined" && react_data && react_data.name) {
                return react_data.name;
            }

            if (typeof window !== "undefined") {
                if (window.react_data && window.react_data.name) {
                    return window.react_data.name;
                }
                if (window.reactData && window.reactData.name) {
                    return window.reactData.name;
                }
            }

            return null;
        }
        """)

        if name:
            return str(name).strip()

    except Exception:
        pass

    return ""


def extract_product_name_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    og_title = soup.find("meta", attrs={"property": "og:title"})
    if og_title and og_title.get("content"):
        return og_title["content"].strip()

    h1 = soup.find("h1")
    if h1:
        text = h1.get_text(strip=True)
        if text:
            return text

    if soup.title:
        text = soup.title.get_text(strip=True)
        if text:
            return text

    return ""


def create_context_and_page(browser):
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        locale="en-US",
        viewport={"width": 1366, "height": 768},
    )

    page = context.new_page()
    page.set_default_navigation_timeout(PAGE_TIMEOUT_MS)
    page.set_default_timeout(PAGE_TIMEOUT_MS)

    return context, page


def random_sleep():
    time.sleep(random.uniform(MIN_REQUEST_SLEEP_SECONDS, MAX_REQUEST_SLEEP_SECONDS))


def crawl_one(page, product_id: str, source_collection: str) -> Dict[str, str]:
    url = build_canonical_url(product_id)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=PAGE_TIMEOUT_MS,
            )

            if response is None:
                status = "no_response"
            else:
                status_code = response.status

                if status_code != 200:
                    status = f"http_error_{status_code}"
                else:
                    try:
                        page.wait_for_load_state("networkidle", timeout=5000)
                    except Exception:
                        pass

                    product_name = extract_product_name_from_react(page)

                    if not product_name:
                        html = page.content()
                        product_name = extract_product_name_from_html(html)

                    if product_name:
                        return {
                            "product_id": product_id,
                            "product_name": product_name,
                            "url": url,
                            "source_collection": source_collection,
                            "crawl_status": "success",
                        }

                    status = "no_name_found"

        except PlaywrightTimeoutError:
            status = "timeout"

        except Exception as e:
            status = f"failed_{type(e).__name__}"

        retryable = (
            status in {"timeout", "no_response"}
            or status.startswith("http_error_5")
            or status == "http_error_429"
            or status.startswith("failed_")
        )

        if attempt < MAX_RETRIES and retryable:
            time.sleep(2 ** attempt)
            continue

        return {
            "product_id": product_id,
            "product_name": "",
            "url": url,
            "source_collection": source_collection,
            "crawl_status": status,
        }

    return {
        "product_id": product_id,
        "product_name": "",
        "url": url,
        "source_collection": source_collection,
        "crawl_status": "unknown_error",
    }


def ensure_output_file():
    output_path = Path(OUTPUT_FILE)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not output_path.exists():
        with open(output_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "product_id",
                "product_name",
                "url",
                "source_collection",
                "crawl_status",
            ])


def append_result(writer, result: Dict[str, str]):
    writer.writerow([
        result["product_id"],
        result["product_name"],
        result["url"],
        result["source_collection"],
        result["crawl_status"],
    ])


def crawl_product_names():
    ensure_output_file()

    input_rows = load_input_rows()
    processed_ids = load_processed_ids()

    remaining_rows = [
        row for row in input_rows
        if row["product_id"] not in processed_ids
    ]

    print(f"Total input rows: {len(input_rows):,}")
    print(f"Already processed: {len(processed_ids):,}")
    print(f"Remaining rows: {len(remaining_rows):,}")
    print(
        f"Config => batch_size={BATCH_SIZE}, "
        f"batch_sleep={BATCH_SLEEP_SECONDS}s, "
        f"request_sleep=({MIN_REQUEST_SLEEP_SECONDS}-{MAX_REQUEST_SLEEP_SECONDS})s, "
        f"retries={MAX_RETRIES}"
    )

    total_rows = 0
    success_rows = 0
    failed_rows = 0
    batch_count = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )

        context, page = create_context_and_page(browser)

        with open(OUTPUT_FILE, "a", encoding="utf-8", newline="") as fout:
            writer = csv.writer(fout)

            for row in remaining_rows:
                total_rows += 1
                batch_count += 1

                product_id = row["product_id"]
                source_collection = row["source_collection"]

                result = crawl_one(page, product_id, source_collection)
                append_result(writer, result)
                fout.flush()

                if result["crawl_status"] == "success":
                    success_rows += 1
                else:
                    failed_rows += 1

                if total_rows % 50 == 0:
                    print(
                        f"Processed: {total_rows:,}/{len(remaining_rows):,} | "
                        f"Success: {success_rows:,} | "
                        f"Failed: {failed_rows:,}"
                    )

                random_sleep()

                if batch_count >= BATCH_SIZE:
                    print(
                        f"Batch completed: {BATCH_SIZE} rows | "
                        f"Rotating browser context and sleeping {BATCH_SLEEP_SECONDS}s..."
                    )

                    try:
                        context.close()
                    except Exception:
                        pass

                    context, page = create_context_and_page(browser)
                    time.sleep(BATCH_SLEEP_SECONDS)
                    batch_count = 0

        try:
            context.close()
        except Exception:
            pass

        browser.close()

    print("Done.")
    print(f"Output file: {OUTPUT_FILE}")
    print(f"Processed this run: {total_rows:,}")
    print(f"Success rows: {success_rows:,}")
    print(f"Failed rows: {failed_rows:,}")


if __name__ == "__main__":
    crawl_product_names()