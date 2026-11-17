"""Wrappers for the Akamai Property Manager API (PAPI)."""

from typing import Any
from urllib.parse import urljoin
from akamai_client import raise_for_status

def get_all_contracts(session, base_url):
    """Fetches all contract IDs associated with the Akamai account."""
    url = urljoin(base_url, "/papi/v1/contracts")
    response = session.get(url)
    response.raise_for_status()
    data = response.json()
    return [item["contractId"] for item in data["contracts"]["items"]]

def get_hostnames(
    session: Any,
    base_url: str,
    contract_id: str | None = None,
    group_id: str | None = None,
    network: str | None = None,
) -> dict:
    """Fetch hostnames, optionally filtered by contract, group, and network."""
    hostnames_url = urljoin(base_url, "/papi/v1/hostnames")
    params = {
        key: value
        for key, value in {
            "contractId": contract_id,
            "groupId": group_id,
            "network": network,
        }.items()
        if value is not None
    }
    response = session.get(hostnames_url, params=params) if params else session.get(hostnames_url)
    raise_for_status(response)
    data = response.json()

    # The hostnames are double, as there are hostname for production and staging environments.
    hostnames = {}
    for item in data.get("hostnames", []).get("items", []):
        hostname = item.get("cnameFrom")
        if hostname:
            hostnames.setdefault(hostname, {}).update(item)
    return hostnames

def get_edgehosts(session: Any, base_url: str, contract_id: str, group_id: str) -> list:
    """Fetch the list of edge hostnames from the Akamai API."""
    edgehosts_url = urljoin(base_url, "/papi/v1/edgehostnames")
    params = {
        "contractId": contract_id,
        "groupId": group_id,
    }
    response = session.get(edgehosts_url, params=params)
    raise_for_status(response)
    data = response.json()
    return data.get("edgeHostnames", {}).get("items", [])


def get_groups(session: Any, base_url: str) -> list:
    """Fetch the list of groups from the Akamai API."""
    groups_url = urljoin(base_url, "/papi/v1/groups")
    response = session.get(groups_url)
    raise_for_status(response)
    data = response.json()
    return data.get("groups", []).get("items", [])


def get_property(
    session: Any,
    base_url: str,
    contract_id: str,
    group_id: str,
    property_id: str,
) -> dict:
    """Fetch the details of a specific Akamai property."""
    property_url = urljoin(base_url, f"/papi/v1/properties/{property_id}")
    params = {
        "contractId": contract_id,
        "groupId": group_id,
    }
    response = session.get(property_url, params=params)
    raise_for_status(response)
    return response.json()


def find_behavior(rules: Any, behavior_name: str) -> Any:
    """
    Recursively searches for all behaviors by name in the given rules.

    Args:
        rules (dict): The rules object to search within.
        behavior_name (str): The name of the behavior to find.

    Returns:
        list: A list of all matching behaviors, empty if none found.
    """
    if isinstance(rules, dict):
        if rules.get("name") == behavior_name:
            return [rules]
        results = []
        for key, value in rules.items():
            results.extend(find_behavior(value, behavior_name))
        return results
    elif isinstance(rules, list):
        results = []
        for item in rules:
            results.extend(find_behavior(item, behavior_name))
        return results
    return []


def find_property_by_name(session: Any, base_url: str, property_name: str) -> Any:
    """Find the active Akamai property version with the given name."""
    search_url = urljoin(base_url, "/papi/v1/search/find-by-value")
    payload = {"propertyName": property_name}
    response = session.post(search_url, json=payload)
    raise_for_status(response)
    data = response.json()

    for version in data.get("versions", {}).get("items", []):
        if version.get("productionStatus") == "ACTIVE":
            return version

    return None


def get_property_rules(
    session: Any,
    base_url: str,
    contract_id: str,
    group_id: str,
    property_id: str,
    prod_version: str,
) -> dict:
    """Fetch the rules for an Akamai property version."""
    endpoint = f"/papi/v1/properties/{property_id}/versions/{prod_version}/rules"
    url = urljoin(base_url, endpoint)
    params = {
        "contractId": contract_id,
        "groupId": group_id,
        "originalInput": "false",
    }
    response = session.get(url, params=params)
    raise_for_status(response)
    return response.json()


def update_property_rules(
    session: Any,
    base_url: str,
    contract_id: str,
    group_id: str,
    property_id: str,
    prop_version: str,
    rules: dict,
) -> dict:
    """Update the rules for an Akamai property version."""
    endpoint = f"/papi/v1/properties/{property_id}/versions/{prop_version}/rules"
    url = urljoin(base_url, endpoint)
    params = {
        "contractId": contract_id,
        "groupId": group_id,
    }
    response = session.put(url, params=params, json=rules)
    raise_for_status(response)
    return response.json()


def activate_property_version(
    session: Any,
    base_url: str,
    contract_id: str,
    group_id: str,
    property_id: str,
    version: str,
    network: str,
    emails: list[str],
    comments: str,
) -> dict:
    """Activate a specific version of an Akamai property."""
    endpoint = f"/papi/v1/properties/{property_id}/activations"
    url = urljoin(base_url, endpoint)
    params = {
        "contractId": contract_id,
        "groupId": group_id,
    }
    body = {
        "propertyVersion": version,
        "notifyEmails": emails,
        "acknowledgeAllWarnings": "true",
        "network": network,
        "note": comments,
        "activationType": "ACTIVATE",
    }
    response = session.post(url, params=params, json=body)
    raise_for_status(response)
    return response.json()


def new_property_version(
    session: Any,
    base_url: str,
    contract_id: str,
    group_id: str,
    property_id: str,
    version: str,
) -> dict:
    """Create a new Akamai property version by cloning an existing version."""
    endpoint = f"/papi/v1/properties/{property_id}/versions"
    url = urljoin(base_url, endpoint)
    params = {
        "contractId": contract_id,
        "groupId": group_id,
    }
    body = {
        "createFromVersion": version,
    }
    response = session.post(url, params=params, json=body)
    raise_for_status(response)
    return response.json()