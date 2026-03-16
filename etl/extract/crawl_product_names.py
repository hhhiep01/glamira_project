import csv
import random
import re
import time
from pathlib import Path
from typing import Dict, Set, List, Tuple
from urllib.parse import urlsplit, urlunsplit

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


INPUT_FILE = "data/interim/distinct_products.csv"
OUTPUT_FILE = "data/interim/product_names_raw.csv"
DEBUG_DIR = "data/debug/product_pages"

BASE_URL = "https://www.glamira.com/catalog/product/view/id/"

BATCH_SIZE = 20
BATCH_SLEEP_SECONDS = 12
MIN_REQUEST_SLEEP_SECONDS = 2.0
MAX_REQUEST_SLEEP_SECONDS = 4.0
PAGE_TIMEOUT_MS = 20000
MAX_RETRIES_PER_URL = 2

LIMIT_ROWS = None


def build_canonical_url(product_id: str) -> str:
    return f"{BASE_URL}{product_id}"


def normalize_url(url: str) -> str:
    return (url or "").strip()


def remove_query_string(url: str) -> str:
    if not url:
        return ""
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def build_candidate_urls(product_id: str, original_url: str = "") -> List[str]:
    candidates = []

    original_url = normalize_url(original_url)
    canonical_url = build_canonical_url(product_id)

    if original_url:
        candidates.append(original_url)

        original_no_query = remove_query_string(original_url)
        if original_no_query and original_no_query not in candidates:
            candidates.append(original_no_query)

    if canonical_url not in candidates:
        candidates.append(canonical_url)

    return candidates


def load_input_rows() -> list[dict]:
    rows = []

    with open(INPUT_FILE, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            product_id = (row.get("product_id") or "").strip()
            source_collection = (row.get("source_collection") or "").strip()
            original_url = (row.get("original_url") or "").strip()

            if not product_id:
                continue

            rows.append({
                "product_id": product_id,
                "source_collection": source_collection,
                "original_url": original_url,
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


def clean_product_name(text: str) -> str:
    if not text:
        return ""

    text = str(text).strip()
    text = re.sub(r"\s+", " ", text)

    # Loại bớt suffix/title rác thường gặp
    garbage_suffixes = [
        r"\s*\|\s*GLAMIRA.*$",
        r"\s*-\s*GLAMIRA.*$",
        r"\s*\|\s*Buy.*$",
        r"\s*-\s*Buy.*$",
        r"\s*\|\s*Online Shop.*$",
        r"\s*-\s*Online Shop.*$",
    ]

    for pattern in garbage_suffixes:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE).strip()

    return text


def is_reasonable_name(text: str) -> bool:
    if not text:
        return False

    text = clean_product_name(text)

    if len(text) < 2:
        return False

    lowered = text.lower()

    bad_values = {
        "glamira",
        "home",
        "product",
        "jewellery",
        "jewelry",
        "buy now",
        "online shop",
    }

    if lowered in bad_values:
        return False

    return True


def english_score(text: str) -> Tuple[int, int, int, int]:
    """
    Score cao hơn = ưu tiên hơn.
    Ưu tiên:
    1) tên hợp lệ
    2) có nhiều ký tự ASCII/English-like
    3) có chữ cái
    4) ít ký tự lạ
    """
    text = clean_product_name(text)

    if not is_reasonable_name(text):
        return (-1, -1, -1, -1)

    ascii_chars = sum(1 for ch in text if ord(ch) < 128)
    alpha_chars = sum(1 for ch in text if ch.isalpha())
    ascii_alpha = sum(1 for ch in text if ch.isalpha() and ord(ch) < 128)
    non_ascii = sum(1 for ch in text if ord(ch) >= 128)

    # bonus nếu có từ khóa thường gặp của tên sản phẩm tiếng Anh
    lowered = text.lower()
    bonus = 0
    english_markers = [
        "ring", "earring", "earrings", "necklace", "bracelet", "pendant",
        "brooch", "anklet", "cufflink", "jewelry", "jewellery", "glamira"
    ]
    if any(word in lowered for word in english_markers):
        bonus += 20

    return (
        bonus + ascii_alpha,   # càng nhiều chữ cái ASCII càng tốt
        ascii_chars,           # tổng ký tự ASCII
        alpha_chars,           # tổng chữ cái
        -non_ascii,            # càng ít non-ascii càng tốt
    )


def choose_best_name(candidates: List[str]) -> str:
    cleaned_candidates = []
    seen = set()

    for name in candidates:
        cleaned = clean_product_name(name)
        if not cleaned:
            continue

        key = cleaned.casefold()
        if key in seen:
            continue
        seen.add(key)

        if is_reasonable_name(cleaned):
            cleaned_candidates.append(cleaned)

    if not cleaned_candidates:
        return ""

    cleaned_candidates.sort(key=english_score, reverse=True)
    return cleaned_candidates[0]


def extract_product_names_from_react(page) -> List[str]:
    candidates = []

    try:
        values = page.evaluate("""
        () => {
            const out = [];

            const pushVal = (v) => {
                if (!v) return;
                if (typeof v === "string") out.push(v);
            };

            const pushObjName = (obj) => {
                if (!obj || typeof obj !== "object") return;
                if (obj.name) pushVal(obj.name);
                if (obj.productName) pushVal(obj.productName);
                if (obj.title) pushVal(obj.title);
                if (obj.product && obj.product.name) pushVal(obj.product.name);
            };

            if (typeof react_data !== "undefined") {
                pushObjName(react_data);
            }

            if (typeof window !== "undefined") {
                pushObjName(window.react_data);
                pushObjName(window.reactData);
                pushObjName(window.__NEXT_DATA__);
                pushObjName(window.__INITIAL_STATE__);
            }

            return out;
        }
        """)

        if isinstance(values, list):
            for value in values:
                if value:
                    candidates.append(str(value).strip())

    except Exception:
        pass

    return candidates


def extract_product_names_from_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    candidates = []

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            import json
            content = script.string or script.get_text()
            if not content:
                continue

            data = json.loads(content)

            if isinstance(data, dict):
                if data.get("name"):
                    candidates.append(str(data["name"]).strip())

                if isinstance(data.get("brand"), dict) and data["brand"].get("name"):
                    candidates.append(str(data["brand"]["name"]).strip())

            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get("name"):
                        candidates.append(str(item["name"]).strip())
        except Exception:
            pass

    og_title = soup.find("meta", attrs={"property": "og:title"})
    if og_title and og_title.get("content"):
        candidates.append(og_title["content"].strip())

    meta_title = soup.find("meta", attrs={"name": "title"})
    if meta_title and meta_title.get("content"):
        candidates.append(meta_title["content"].strip())

    h1 = soup.find("h1")
    if h1:
        text = h1.get_text(strip=True)
        if text:
            candidates.append(text)

    if soup.title:
        text = soup.title.get_text(strip=True)
        if text:
            candidates.append(text)

    return candidates


def extract_best_product_name(page) -> str:
    candidates = []

    react_candidates = extract_product_names_from_react(page)
    candidates.extend(react_candidates)

    try:
        html = page.content()
        html_candidates = extract_product_names_from_html(html)
        candidates.extend(html_candidates)
    except Exception:
        pass

    return choose_best_name(candidates)


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


def warmup_session(page):
    try:
        page.goto(
            "https://www.glamira.com/",
            wait_until="domcontentloaded",
            timeout=PAGE_TIMEOUT_MS,
        )
        time.sleep(random.uniform(1.5, 3.0))
    except Exception:
        pass


def random_sleep():
    time.sleep(random.uniform(MIN_REQUEST_SLEEP_SECONDS, MAX_REQUEST_SLEEP_SECONDS))


def save_debug_artifacts(page, product_id: str, suffix: str):
    debug_dir = Path(DEBUG_DIR)
    debug_dir.mkdir(parents=True, exist_ok=True)

    screenshot_path = debug_dir / f"{product_id}_{suffix}.png"
    html_path = debug_dir / f"{product_id}_{suffix}.html"

    try:
        page.screenshot(path=str(screenshot_path), full_page=True)
    except Exception:
        pass

    try:
        html = page.content()
        html_path.write_text(html, encoding="utf-8")
    except Exception:
        pass


def try_single_url(page, url: str) -> tuple[str, str]:
    for attempt in range(1, MAX_RETRIES_PER_URL + 1):
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
                        page.wait_for_load_state("networkidle", timeout=6000)
                    except Exception:
                        pass

                    product_name = extract_best_product_name(page)

                    if product_name:
                        return "success", product_name

                    status = "no_name_found"

        except PlaywrightTimeoutError:
            status = "timeout"
        except Exception as e:
            status = f"failed_{type(e).__name__}"

        retryable = (
            status in {"timeout", "no_response", "http_error_403", "http_error_429"}
            or status.startswith("http_error_5")
            or status.startswith("failed_")
        )

        if attempt < MAX_RETRIES_PER_URL and retryable:
            time.sleep((2 ** attempt) + random.uniform(0.5, 1.5))
            continue

        return status, ""

    return "unknown_error", ""


def crawl_one(page, product_id: str, source_collection: str, original_url: str = "") -> Dict[str, str]:
    candidate_urls = build_candidate_urls(product_id, original_url)
    last_status = "unknown_error"

    for idx, url in enumerate(candidate_urls, start=1):
        status, product_name = try_single_url(page, url)

        if status == "success":
            return {
                "product_id": product_id,
                "product_name": product_name,
                "url": url,
                "source_collection": source_collection,
                "crawl_status": "success",
            }

        last_status = status

        if status in {"http_error_403", "timeout", "no_response"}:
            try:
                save_debug_artifacts(page, product_id, f"try{idx}")
            except Exception:
                pass

        time.sleep(random.uniform(1.0, 2.0))

    return {
        "product_id": product_id,
        "product_name": "",
        "url": candidate_urls[-1] if candidate_urls else "",
        "source_collection": source_collection,
        "crawl_status": last_status,
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

    if not remaining_rows:
        print("Nothing to crawl.")
        return

    total_rows = 0
    success_rows = 0
    failed_rows = 0
    batch_count = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )

        context, page = create_context_and_page(browser)
        warmup_session(page)

        with open(OUTPUT_FILE, "a", encoding="utf-8", newline="") as fout:
            writer = csv.writer(fout)

            for row in remaining_rows:
                total_rows += 1
                batch_count += 1

                result = crawl_one(
                    page=page,
                    product_id=row["product_id"],
                    source_collection=row["source_collection"],
                    original_url=row.get("original_url", ""),
                )

                append_result(writer, result)
                fout.flush()

                if result["crawl_status"] == "success":
                    success_rows += 1
                else:
                    failed_rows += 1

                if total_rows % 20 == 0 or total_rows == len(remaining_rows):
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

                    time.sleep(BATCH_SLEEP_SECONDS)
                    context, page = create_context_and_page(browser)
                    warmup_session(page)
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