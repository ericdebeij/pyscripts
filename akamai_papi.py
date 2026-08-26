"""Wrappers for the Akamai Property Manager API (PAPI)."""

from typing import Any
from urllib.parse import urljoin


def get_hostnames(session: Any, base_url: str) -> list:
    """Fetch the list of hostnames from the Akamai API."""
    hostnames_url = urljoin(base_url, "/papi/v1/hostnames")
    response = session.get(hostnames_url)
    if response.status_code > 400:
        print(f"Error fetching hostnames: {response.status_code}")
        print(f"Response content: {response.text}")
    response.raise_for_status()
    data = response.json()
    return data.get("hostnames", []).get("items", [])


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
    print(f"Find property - status code: {response.status_code}")
    print(f"Response content: {response.text}")
    response.raise_for_status()
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
    print(f"Get property rules - status code: {response.status_code}")
    if response.status_code > 400:
        print(f"Response content: {response.text}")
    response.raise_for_status()
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
    print(f"Update property rules - status code: {response.status_code}")
    if response.status_code > 400:
        print(f"Response content: {response.text}")
    response.raise_for_status()
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
    print(f"Activate status code: {response.status_code}")
    print(f"Response content: {response.text}")
    response.raise_for_status()
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
    print(f"New property version status code: {response.status_code}")
    if response.status_code > 400:
        print(f"Response content: {response.text}")
    response.raise_for_status()
    return response.json()