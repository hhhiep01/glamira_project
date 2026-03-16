import IP2Location

class IPToLocation:
    def __init__(self, bin_path: str):
        self.ip2 = IP2Location.IP2Location(bin_path)

    def lookup(self, ip: str) -> dict:
        rec = self.ip2.get_all(ip)
        return {
            "ip": ip,
            "country_short": rec.country_short,
            "country_long": rec.country_long,
            "region": rec.region,
            "city": rec.city,
        }
