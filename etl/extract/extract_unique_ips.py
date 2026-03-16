

from collections.abc import Collection, Iterator
from typing import Optional


def iter_ips(
        raw_col : Collection,
        limit: Optional[int] = None,
        batch_size: int = 1000
        ) -> Iterator[str]:
    
    query = {"ip": {"$exists": True, "$nin": [None, ""]}}
    projection = {"ip": 1, "_id": 0}

    cursor = raw_col.find(query, 
                          projection,  
                          no_cursor_timeout=True,
                          batch_size=batch_size,
                          )
    try:
        n = 0
        for doc in cursor:
            ip = doc.get("ip")
            if isinstance(ip,str) and ip:
                yield ip
                n += 1
                if limit is not None and n >= limit:
                    break
    finally:
        cursor.close()