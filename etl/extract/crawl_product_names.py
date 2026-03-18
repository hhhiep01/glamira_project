import asyncio
import csv
import random
from pathlib import Path
from typing import Dict, List, Set

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


INPUT_FILE = "data/interim/distinct_products.csv"
OUTPUT_FILE = "data/interim/product_names_raw.csv"

BASE_URL = "https://www.glamira.com/catalog/product/view/id/"

MAX_CONCURRENCY = 6
CHUNK_SIZE = 30
PAGE_TIMEOUT_MS = 12000
MAX_RETRIES = 2
LIMIT_ROWS = None

MIN_REQUEST_SLEEP_SECONDS = 0.3
MAX_REQUEST_SLEEP_SECONDS = 0.8
RETRY_SLEEP_MIN_SECONDS = 1.0
RETRY_SLEEP_MAX_SECONDS = 2.0
CHUNK_SLEEP_SECONDS = 3.0


def build_canonical_url(product_id: str) -> str:
    return f"{BASE_URL}{product_id}"


def load_input_rows() -> List[dict]:
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
            crawl_status = (row.get("crawl_status") or "").strip()

            if product_id and crawl_status == "success":
                processed_ids.add(product_id)

    return processed_ids


def ensure_output_file():
    output_path = Path(OUTPUT_FILE)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    need_header = False

    if not output_path.exists():
        need_header = True
    elif output_path.stat().st_size == 0:
        need_header = True

    if need_header:
        with open(output_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "product_id",
                "product_name",
                "url",
                "source_collection",
                "crawl_status",
            ])


def append_results_to_csv(results: List[Dict[str, str]]):
    with open(OUTPUT_FILE, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)

        for result in results:
            writer.writerow([
                result["product_id"],
                result["product_name"],
                result["url"],
                result["source_collection"],
                result["crawl_status"],
            ])


def chunked(items: List[dict], chunk_size: int) -> List[List[dict]]:
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def classify_exception(exc: Exception) -> str:
    msg = str(exc).lower()

    socket_keywords = [
        "socket",
        "connection reset",
        "connection aborted",
        "connection closed",
        "broken pipe",
        "target page, context or browser has been closed",
        "net::err_connection_reset",
        "net::err_connection_closed",
        "net::err_internet_disconnected",
        "net::err_network_changed",
    ]

    for keyword in socket_keywords:
        if keyword in msg:
            return "socket_error"

    return f"failed_{type(exc).__name__}"


async def extract_product_name(page) -> str:
    try:
        name = await page.evaluate("""
        () => {
            if (typeof react_data !== "undefined" && react_data?.name) return react_data.name;
            if (window.react_data?.name) return window.react_data.name;
            if (window.reactData?.name) return window.reactData.name;

            const og = document.querySelector('meta[property="og:title"]');
            if (og?.content) return og.content.trim();

            const h1 = document.querySelector('h1');
            if (h1?.innerText) return h1.innerText.trim();

            if (document.title) return document.title.trim();

            return null;
        }
        """)
        return str(name).strip() if name else ""
    except Exception:
        return ""


async def create_context_and_page(browser):
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        locale="en-US",
        viewport={"width": 1366, "height": 768},
    )

    page = await context.new_page()
    page.set_default_navigation_timeout(PAGE_TIMEOUT_MS)
    page.set_default_timeout(PAGE_TIMEOUT_MS)

    async def route_handler(route):
        if route.request.resource_type in {"image", "font", "media"}:
            await route.abort()
        else:
            await route.continue_()

    await page.route("**/*", route_handler)

    return context, page


async def crawl_one(browser, row: dict, semaphore: asyncio.Semaphore, input_index: int) -> Dict[str, str]:
    async with semaphore:
        await asyncio.sleep(random.uniform(MIN_REQUEST_SLEEP_SECONDS, MAX_REQUEST_SLEEP_SECONDS))

        product_id = row["product_id"]
        source_collection = row["source_collection"]
        url = build_canonical_url(product_id)

        context = None

        try:
            context, page = await create_context_and_page(browser)

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    response = await page.goto(
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
                            product_name = await extract_product_name(page)

                            if product_name:
                                return {
                                    "input_index": input_index,
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
                    status = classify_exception(e)

                retryable = (
                    status in {"timeout", "no_response", "socket_error"}
                    or status.startswith("http_error_5")
                    or status == "http_error_429"
                    or status.startswith("failed_")
                )

                if attempt < MAX_RETRIES and retryable:
                    await asyncio.sleep(random.uniform(RETRY_SLEEP_MIN_SECONDS, RETRY_SLEEP_MAX_SECONDS))
                    continue

                return {
                    "input_index": input_index,
                    "product_id": product_id,
                    "product_name": "",
                    "url": url,
                    "source_collection": source_collection,
                    "crawl_status": status,
                }

        finally:
            if context:
                try:
                    await context.close()
                except Exception:
                    pass

        return {
            "input_index": input_index,
            "product_id": product_id,
            "product_name": "",
            "url": url,
            "source_collection": source_collection,
            "crawl_status": "unknown_error",
        }


async def crawl_chunk(browser, chunk_rows: List[dict], start_index: int) -> List[Dict[str, str]]:
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    tasks = [
        crawl_one(browser, row, semaphore, start_index + i)
        for i, row in enumerate(chunk_rows)
    ]

    results = await asyncio.gather(*tasks)
    results.sort(key=lambda x: x["input_index"])
    return results


async def crawl_product_names():
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
        f"Config => concurrency={MAX_CONCURRENCY}, "
        f"chunk_size={CHUNK_SIZE}, "
        f"timeout={PAGE_TIMEOUT_MS}ms, "
        f"retries={MAX_RETRIES}"
    )

    if not remaining_rows:
        print("No remaining rows to crawl.")
        return

    total_processed = 0
    total_success = 0
    total_failed = 0

    chunks = chunked(remaining_rows, CHUNK_SIZE)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )

        try:
            start_index = 0

            for chunk_index, chunk_rows in enumerate(chunks, start=1):
                print(
                    f"\n=== Chunk {chunk_index}/{len(chunks)} | "
                    f"Rows in chunk: {len(chunk_rows)} ==="
                )

                results = await crawl_chunk(
                    browser=browser,
                    chunk_rows=chunk_rows,
                    start_index=start_index,
                )

                append_results_to_csv(results)

                chunk_success = sum(1 for r in results if r["crawl_status"] == "success")
                chunk_failed = len(results) - chunk_success

                total_processed += len(results)
                total_success += chunk_success
                total_failed += chunk_failed
                start_index += len(chunk_rows)

                print(
                    f"Processed: {total_processed:,}/{len(remaining_rows):,} | "
                    f"Chunk success: {chunk_success:,} | "
                    f"Chunk failed: {chunk_failed:,}"
                )

                if chunk_index < len(chunks):
                    print(f"Sleeping {CHUNK_SLEEP_SECONDS}s before next chunk...")
                    await asyncio.sleep(CHUNK_SLEEP_SECONDS)

        finally:
            try:
                await browser.close()
            except Exception:
                pass

    print("\nDone.")
    print(f"Output file: {OUTPUT_FILE}")
    print(f"Processed this run: {total_processed:,}")
    print(f"Success rows: {total_success:,}")
    print(f"Failed rows: {total_failed:,}")


if __name__ == "__main__":
    asyncio.run(crawl_product_names())