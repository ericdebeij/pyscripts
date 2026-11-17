"""
List Akamai edgehosts or report their usage with the list and usage subcommands.
The usage command checks all edgehosts to see if they are still necessary.
The first step is to retrieve all edgehosts from the Akamai contracts, the contract is an optional parameter.
The next step is to check if the edgehost is being used in any property
The next step is to check if the edgehost is being used in any dns record of the zones in akamai
The reporting step will show all edgehosts, where they are used and if they can be removed
It ends with a summary of the edgehosts which are not valid or can be removed
"""
import argparse
import json
from akamai_papi import get_hostnames
import os
from typing import Any

from akamai_client import get_akamai_session
from akamai_papi import get_edgehosts, get_groups
from akamai_edns import zone_apex
from urllib.parse import urljoin

try:
    import dns.resolver
except ImportError:  # pragma: no cover - optional dependency
    dns = None



def get_cname_chain(hostname: str, max_hops: int = 10) -> list[str]:
    """
    Resolve the CNAME chain for a host and return the list of names visited.
    This is useful when checking whether a hostname ultimately resolves to an
    Akamai edgehost or another external service.
    """
    if dns is None:
        raise RuntimeError("dnspython is required for CNAME resolution: pip install dnspython")

    current = hostname.rstrip(".").lower()
    chain: list[str] = []
    seen: set[str] = set()

    for _ in range(max_hops):
        if current in seen:
            break
        seen.add(current)
        chain.append(current)

        try:
            answers = dns.resolver.resolve(current, "CNAME", lifetime=5)
        except (
            dns.resolver.NoAnswer,
            dns.resolver.NXDOMAIN,
            dns.resolver.NoNameservers,
            dns.exception.DNSException, # type: ignore
        ):
            break

        if not answers:
            break

        target = str(answers[0].target).rstrip(".").lower() # type: ignore
        if target == current:
            break
        current = target

    return chain



def get_all_edgehosts(
    session: Any,
    base_url: str,


    contract_id: str | None = None,
) -> list:
    """
    Retrieve the combined list of all edgehosts
    optionally filtered by contractId.
    Each edgehost in the result gets a "contractId" field.
    """

    groups = get_groups(session, base_url)
    all_edgehosts: list = []

    for group in groups:
        current_group_id = group.get("groupId")
        # if group_id is not None and current_group_id != group_id:
        #     continue

        # we only need the top-level groups
        parent_group_id = group.get("parentGroupId")

        if parent_group_id is not None:
            continue

        for current_contract_id in group.get("contractIds", []):
            if contract_id is not None and current_contract_id != contract_id:
                continue

            edgehosts = get_edgehosts(session, base_url, current_contract_id, current_group_id)
            
            if edgehosts:
                for edgehost in edgehosts:
                    edgehost["groupId"] = current_group_id
                    edgehost["contractId"] = current_contract_id
                all_edgehosts.extend(edgehosts)

    return all_edgehosts

EDGEHOST_DOMAINS = ["akamaized.net", "edgekey.net", "edgesuite.net", "akamaized-staging.net", "edgekey-staging.net", "edgesuite-staging.net"]
def dig_edgehost(hostname: str) -> str | None:
    """ find the edgehost based on the real CNAME """

    # check the CNAME chain for this host
    if any(hostname.endswith(f".{domain}") for domain in EDGEHOST_DOMAINS):
        return hostname

    if hostname.startswith("*"):
        hostname = "a" + hostname[1:]

    cname_chain = get_cname_chain(hostname)

    for eh in cname_chain:
        if any(eh.endswith(f".{domain}") for domain in EDGEHOST_DOMAINS):
            return eh.replace("-staging", "")

def pm_edgehosts(
    session: Any,
    base_url: str,
    domains: list[str] | None = None,
    dumpjson: bool = False,
) -> dict:
    """Retrieve the host edghost mapping from property manager"""
    hosts = get_hostnames(session, base_url)
    if dumpjson:
        with open("hostnames.json", "w", encoding="utf-8") as file:
            json.dump(hosts, file, indent=2)

    # If domains are specified, filter the hostnames accordingly
    hosts_filter = {}
    for hn, host in hosts.items():
        if domains and not (any(hn.endswith(f".{domain}") for domain in domains)):
            continue

        hosts_filter[hn] = host
    hosts = hosts_filter

    pmhosts = {}
    for hostname, host in hosts.items():
        pmhosts[hostname] = {}
        if "stagingCnameTo" in host:
            pmhosts[hostname]["PM staging"] = host["stagingCnameTo"]
        if "productionCnameTo" in host:
            pmhosts[hostname]["PM production"] = host["productionCnameTo"]

    return pmhosts


def edgehosts_usage(
    session: Any,
    base_url: str,
    contract_id: str | None = None,
    domains: list[str] | None = None,
    dumpjson: bool = False,
) -> tuple[dict, dict]:
    """Return the edgehost and hostname maps, optionally exporting JSON files."""

    print("Retrieving all edgehosts from Akamai...")
    edgehosts = get_all_edgehosts(session, base_url, contract_id=contract_id)

    # Get the hostnames from the property manager
    print("Retrieving hostnames from the property manager...")
    hosts = pm_edgehosts(session, base_url, domains=domains, dumpjson=dumpjson)

    # Get the zone_apex mappings from DNS
    print("Retrieving zone apex mappings from DNS...")
    zoneapex = zone_apex(session, base_url, zonename=domains)

    print("DIGing the CNAME chain...")
    for host in hosts:
        if host in zoneapex:
            hosts[host]["zoneapex"] = zoneapex[host]

        dig = dig_edgehost(hostname=host)
        if dig:
            hosts[host]["dig"] = dig

    print("Building edgehost map...")
    edgehost_map = {}
    for eh in edgehosts:
        if domains and not any(eh["domainPrefix"].endswith(f".{domain}") for domain in domains):
            continue
        edgehost = eh["edgeHostnameDomain"]
        edgehost_map[edgehost] = eh

    print("Mapping hostnames to edgehosts...")
    for hostname, hostinfo in hosts.items():
        for usage, eh in hostinfo.items():
            edgehost_map.setdefault(eh, {}).setdefault("usage", {}).setdefault(hostname,[]).append(usage)

    if dumpjson:
        print("Writing the export files...")
        with open("edgehosts_map.json", "w", encoding="utf-8") as file:
            json.dump(edgehost_map, file, indent=2)
        with open("hosts_map.json", "w", encoding="utf-8") as file:
            json.dump(hosts, file, indent=2)

    return edgehost_map, hosts


def usage_report(edgehost_map: dict, hosts: dict) -> None:
    """Print the usage report from the supplied in-memory maps."""

    hosts_not_used = {}

    hostgroup = {}
    active_nodns = {}
    for edgehost, eh_info in edgehost_map.items():
        usage = eh_info.get("usage", {})
        if len(usage) == 0:
            hosts_not_used[edgehost] = eh_info
        print(f"Edgehost: {edgehost}")

        
        for hostname, usage_list in usage.items():
            if not("dig" in usage_list or "zoneapex" in usage_list):
                active_nodns.setdefault(hostname, []).append(hostname)

            usage_str = ','.join(usage_list)
            hostgroup.setdefault(usage_str, []).append(hostname)
            print(f".   Host: {hostname}, Usage: {usage_str}")

    print("\n\nHosts not used:")
    for edgehost, eh_info in hosts_not_used.items():
        print(f"Edgehost: {edgehost}")
        for hostname, usage_list in eh_info.get("usage", {}).items():
            print(f".   Host: {hostname}, Usage: {','.join(usage_list)}")

    print("\n\nHosts grouped by usage:")
    for usage_str, hostnames in hostgroup.items():
        print(f"Usage: {usage_str}")
        for hostname in hostnames:
            print(f".   Host: {hostname}")

    print("\n\nActive no DNS hosts:")
    for hostname, _ in active_nodns.items():
        print(f".   {hostname}")

def main() -> None:
    parser = argparse.ArgumentParser(description="List Akamai edgehosts and check their usage")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List all edgehosts as JSON")
    list_parser.add_argument("--contract", help="Filter by Akamai contract ID")

    usage_parser = subparsers.add_parser("usage", help="Retrieve and report edgehost usage")
    usage_parser.add_argument("--contract", help="Filter by Akamai contract ID")
    usage_parser.add_argument(
        "--dumpjson",
        action="store_true",
        help="Write hostname and edgehost usage data to JSON files",
    )
    usage_parser.add_argument(
        "--domains",
        nargs="+",
        default=None,
        metavar="DOMAIN",
        help="One or more domains to check (default: ALL)",
    )

    args = parser.parse_args()

    session, base_url = get_akamai_session()

    if args.command == "list":
        edgehosts = get_all_edgehosts(session, base_url, contract_id=args.contract)
        print(json.dumps(edgehosts, indent=2))
    elif args.command == "usage":
        edgehost_map, hosts = edgehosts_usage(
            session, base_url, contract_id=args.contract,
            domains=args.domains, dumpjson=args.dumpjson,
        )
        usage_report(edgehost_map, hosts)


if __name__ == "__main__":
    main()

