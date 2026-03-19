from etl.extract.extract_unique_ips import iter_ips
from etl.load.mongo_loader import upsert_ip_locations, build_location_doc
from etl.transform.ip_to_location import IPToLocation
from etl.transform.ip_cleaner import clean_ip
from pymongo import MongoClient
import yaml
from pathlib import Path

def load_config(path="config/config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main():
    cfg = load_config()

    client = MongoClient(cfg["mongo"]["uri"])
    db = client[cfg["mongo"]["db"]]
    raw_col = db[cfg["mongo"]["raw_collection"]]
    out_col = db[cfg["mongo"]["ip_collection"]]

    mapper = IPToLocation(cfg["ip2location"]["bin_path"])
    
    docs_to_upsert = []
    seen_ips = set()
    
    unique_ips = 0
    processed = 0
    batch_size = 1000
    for raw_ip in iter_ips(raw_col, limit = None):
        processed +=1
        ip = clean_ip(raw_ip)
        if ip is None:  
            continue
        
        if ip in seen_ips:
            continue
        seen_ips.add(ip)
        unique_ips += 1
        
        location_data = mapper.lookup(ip)
        doc = build_location_doc(ip, location_data)
        docs_to_upsert.append(doc)

        if len(docs_to_upsert) >= batch_size:
            upsert_ip_locations(out_col, docs_to_upsert)
            docs_to_upsert.clear()

        if processed % 10000 == 0:
            print(f"Processed: {processed:,} | Unique IPs: {unique_ips:,}")

        
    if docs_to_upsert:
        upsert_ip_locations(out_col, docs_to_upsert)
        
    print("Pipeline finished")
    print("Processed raw records:", processed)
    print("Unique cleaned IPs:", unique_ips)
    print("Output collection:", cfg["mongo"]["ip_collection"])

if __name__ == "__main__":
    main()