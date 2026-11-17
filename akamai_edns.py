# Akamai Edge DNS wrappers and support utilities
# Akamai DNS API's are described in the akamai documentation which can be found here: https://techdocs.akamai.com/edge-dns/reference/api-summary.md
# 
from akamai_client import get_akamai_session, raise_for_status

from typing import Any
from urllib.parse import urljoin
import json

def list_zones(session: Any, base_url: str) -> list:
    """
    Retrieve the list of DNS zones from Akamai Edge DNS.
    https://techdocs.akamai.com/edge-dns/reference/get-zones
    """
    zones_url = urljoin(base_url, "/config-dns/v2/zones")
    response = session.get(zones_url)
    raise_for_status(response)
    data = response.json()
    return data.get("zones", [])

def get_record_sets(session: Any, base_url: str, zonename: str, types: list | None = None) -> list:
    """
    Retrieve the list of record sets for a specific DNS zone from Akamai Edge DNS.
    https://techdocs.akamai.com/edge-dns/reference/get-record-sets
    """
    records_url = urljoin(base_url, f"/config-dns/v2/zones/{zonename}/recordsets")
    params = {}
    if types:
        params["types"] = ",".join(types)
    response = session.get(records_url, params=params)
    raise_for_status(response)
    data = response.json()
    return data.get("recordsets", [])

def list_record_set_types(session: Any, base_url: str, zonename: str) -> list:
    """
    Retrieve the list of record set types for a specific DNS zone from Akamai Edge DNS.
    https://techdocs.akamai.com/edge-dns/reference/get-record-set-types
    """
    types_url = urljoin(base_url, f"/config-dns/v2/data/recordset-types")


    response = session.get(types_url, params={"zone": zonename})

    raise_for_status(response)
    data = response.json()
    return data.get("types", [])

def zone_apex(session: Any, base_url: str, zonename: list[str]|None = None) -> dict:
    """
    Get all zones and extract the records with zone APEX records either AKAMAICDN or AKAMAITLS
    returns all fully qualified hostnames with their virtual CNAME
    """
    zones = list_zones(session, base_url)
    apex_records = {}
    for zone in zones:
        zname = zone["zone"]
        zonetype = zone["type"]

        if zonename and zname not in zonename:
            continue

        if zonetype in ["PRIMARY", "SECONDARY"]:
            recordsets = get_record_sets(session, base_url, zname, types=["AKAMAICDN", "AKAMAITLS"])
            for record in recordsets:
                apex_records[record["name"]] = ",".join(record["rdata"])

    return apex_records

if __name__ == "__main__":
    # Example usage
    session, base_url = get_akamai_session()  # Assuming you have a function to get the session
    zones = list_zones(session, base_url)

    with open("zones.json", "w") as f:
        json.dump(zones, f, indent=2)

#recordsets = get_record_sets(session, base_url, "a.dosm.nl", types=["AKAMAICDN", "AKAMAITLC"])
#print(json.dumps(recordsets, indent=2))

