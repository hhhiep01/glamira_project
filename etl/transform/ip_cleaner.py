

import ipaddress
import re


def clean_ip(value: str | None) -> str | None:
    if value is None:
        return None
    
    ip = value.strip()
    ip = ip.strip('"').strip("'")
    if "," in ip:
        ip = ip.split(",")[0].strip()
    
    if re.match(r"^\d{1,3}(\.\d{1,3}){3}:\d+$", ip):
        ip = ip.split(":")[0]

    try:
        ipaddress.ip_address(ip)    
        return ip
    except ValueError:
        return None
