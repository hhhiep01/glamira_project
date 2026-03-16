from pymongo import UpdateOne
from pymongo.collection import Collection
from datetime import datetime, timezone
import yaml
from pymongo import MongoClient


def build_location_doc(ip: str, location_data: dict) -> dict:
    return {
        "_id": ip,
        "ip": ip,
        "country_short": location_data.get("country_short"),
        "country_long": location_data.get("country_long"),
        "region": location_data.get("region"),
        "city": location_data.get("city"),
        "updated_at": datetime.now(timezone.utc),
        "source": "ip2location",
    }

def upsert_ip_locations(out_col : Collection, docs:list[dict]):
    if not docs:
        return
    
    operations = []
    for doc in docs:
        operations.append(
            UpdateOne(
                {"_id": doc["_id"]},
                {"$set": doc},
                upsert=True
            )
        )
    
    if operations:
        out_col.bulk_write(operations)



def load_config(path="config/config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main():
    cfg = load_config()
    client = MongoClient(cfg["mongo"]["uri"])
    db = client[cfg["mongo"]["db"]]
    out_col = db[cfg["mongo"]["ip_collection"]]

    # Example usage
    ip = "37.170.17.183"
    location_data = {
        "country_short": "US",
        "country_long": "United States",
        "region": "California",
        "city": "Mountain View"
    }
    doc = build_location_doc(ip, location_data)
    upsert_ip_locations(out_col, [doc])

if __name__ == "__main__":
    main()