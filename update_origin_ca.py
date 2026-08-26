"""
This script updates the origin settings of an Akamai property using the PAPI API.
It finds the property by name, retrieves its rules, updates the origin settings, and optionally activates the new version.
Usage:
    python update-origin-ca.py
Files used:
- .edgerc: Contains the Akamai API credentials. They should be set up in the user's home directory in the [default] section
-- The API credentials must have READ-WRITE access to the entire contract as we search for the property by name.
- origin_settings.json: Contains the new origin settings to apply.

This is an example script and should be used with caution. Always test in a staging environment before applying changes to production.

Customize the following variables in the main block:
- property_name: The name of the Akamai property to update.
- origin_settings: A dictionary containing the new origin settings to apply.

Best way to create the origin_settings.json is by grabbing it from an existing property with the right CA settings in place.
In property manager, navigate to the property, go to the "View JSON section in the Property Configuration Settings" 
and copy the necessary option fields to a file called origin_settings.json.

Fields needed in the origin_settings.json are:
- originCertsToHonor
- standardCertificateAuthorities
- customnCertificateAuthorities

Data will be stored in the ./data directory, which will be created if it doesn't exist.
Use diff tools to compare the original and updated rules to ensure the changes are as expected.
"""
import json
import os
from typing import Any

from akamai_client import get_akamai_session
from urllib.parse import urljoin

def find_property_by_name(session, base_url, property_name: str) -> Any:
    """
   Finds an Akamai property by its name.

   API Endpoint: {base_url}/papi/v1/search/find-by-value
    Method: POST
    Body: {"propertyName": property_name}
   Returns:
       dict: Property details if found, else None.
    """
    
    search_url = urljoin(base_url, "/papi/v1/search/find-by-value")
    payload = {"propertyName": property_name}
    response = session.post(search_url, json=payload)
    print(f"Find property - status code: {response.status_code}")
    #if response.status_code > 400:
    print(f"Response content: {response.text}")
    response.raise_for_status()
    data = response.json()

    for version in data.get("versions", {}).get("items", []):
        if version.get("productionStatus") == "ACTIVE":
            return version

    return None

def get_property_rules(session, base_url, contract_id: str, group_id: str, property_id: str, prod_version: str) -> dict:
    """
    Fetches Akamai PAPI property versions.
    
    API Endpoint: /papi/v1/properties/{property_id}/versions/{prod_version}/rules?contractId={contract_id}&groupId={group_id}
    Method: GET

    """
    endpoint = f"/papi/v1/properties/{property_id}/versions/{prod_version}/rules"
    url = urljoin(base_url, endpoint)
    params = {
        "contractId": contract_id,
        "groupId": group_id,
        "originalInput": "false"
    }
    response = session.get(url, params=params)
    print(f"Get property rules - status code: {response.status_code}")
    if response.status_code > 400:
        print(f"Response content: {response.text}")
    response.raise_for_status()
    return response.json()

def update_property_rules(session, base_url, contract_id: str, group_id: str, property_id: str, prop_version: str, rules: dict) -> dict:    
    """
    Updates Akamai PAPI property rules.

    API Endpoint: /papi/v1/properties/{property_id}/versions/{prop_version}/rules?contractId={contract_id}&groupId={group_id}
    Method: PUT

    Args:
        session: The requests session object.
        base_url (str): The base URL for the Akamai API.
        contract_id (str): The contract ID of the property.
        group_id (str): The group ID of the property.
        property_id (str): The property ID to update.
        prop_version (str): The version number to update.
        rules (dict): The updated rules object.

    Returns:
        dict: The response from the update operation.
    """
    endpoint = f"/papi/v1/properties/{property_id}/versions/{prop_version}/rules"
    url = urljoin(base_url, endpoint)
    params = {
        "contractId": contract_id,
        "groupId": group_id
    }
    response = session.put(url, params=params, json=rules)
    print(f"Update property rules - status code: {response.status_code}")
    if response.status_code > 400:
        print(f"Response content: {response.text}")
    response.raise_for_status()
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

def read_json_file(file_path: str) -> Any:
    """
    Reads a JSON file and returns its content.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        any: The content of the JSON file.
    """
    with open(file_path, "r") as f:
        return json.load(f)


def change_property_rules(session, base_url, data_dir: str, rules: Any,property_name: str, property_details: dict, origin_settings: dict) -> int:
    """
    Updates the origin settings in the property rules.
    The fields in the origin options will be updated with the values from the origin_settings dictionary.

    Args:
        session: The requests session object.
        base_url (str): The base URL for the Akamai API.
        data_dir (str): The directory to store the JSON files.
        rules (dict): The property rules object.
        property_name (str): The name of the property.
        property_details (dict): The details of the property, including the version.
        origin_settings (dict): The origin settings to update.

    Returns:
        int: The number of behaviors updated.
    """

    behaviors = find_behavior(rules, "origin")
    json.dump(behaviors, open(f"{data_dir}/origins_{property_name}_{property_details['propertyVersion']}.json", "w"), indent=4)
    #print(f"Found behaviors: {json.dumps(behaviors, indent=4)}")
    iUpdated = 0
    for behavior in behaviors:
        options = behavior["options"]
        
        before_update = json.dumps(options)
        if options.get("originType") != "CUSTOMER":
            print(f"Origin type {options.get('originType')} skipped")
            continue

        print(f"Origin hostname: {options.get('hostname')}")
        # Update the origin settings
        options.update(origin_settings)
        after_update = json.dumps(options)
        if before_update != after_update:
            iUpdated += 1

    print(f"Updated {iUpdated} behaviors for property: {property_name}")
    json.dump(behaviors, open(f"{data_dir}/origins_{property_name}_new.json", "w"), indent=4)

    return iUpdated

def activate_property_version(session, base_url, contract_id: str, group_id: str, property_id: str, version: str, network: str, emails: list[str], comments: str) -> dict:
    """
    Activate a specific version of an Akamai property.

    API Endpoint: /papi/v1/properties/{propertyId}/activations?contractId={contract_id}&groupId={group_id}
    Method: POST

    Args:
        session: The requests session object.
        base_url (str): The base URL for the Akamai API.
        contract_id (str): The contract ID of the property.
        group_id (str): The group ID of the property.
        property_id (str): The property ID to activate.
        version (str): The version number to activate.
        network (str): The network to activate on.
        emails (list[str]): The list of email addresses to notify.
        comments (str): Activation comments.

    Returns:
        dict: The response from the activation operation.
    """
    endpoint = f"/papi/v1/properties/{property_id.replace('prp_', '')}/activations"
    url = urljoin(base_url, endpoint)
    params = {
        "contractId": contract_id,
        "groupId": group_id}
    body = {
        "propertyVersion": version,
        "notifyEmails": emails,
        "acknowledgeAllWarnings": "true",
        "network": network,
        "note": comments,
        "activationType": "ACTIVATE"
    }
    response = session.post(url, params=params, json=body)
    print(f"Activate status code: {response.status_code}")
    print(f"Response content: {response.text}")
    response.raise_for_status()
    return response.json()

def new_property_version(session, base_url, contract_id: str, group_id: str, property_id: str, version: str) -> dict:
    """
    Create a new version of an Akamai property.

    API Endpoint: /papi/v1/properties/{propertyId}/versions?contractId={contract_id}&groupId={group_id}
    Method: POST

    Args:
        session: The requests session object.
        base_url (str): The base URL for the Akamai API.
        contract_id (str): The contract ID of the property.
        group_id (str): The group ID of the property.
        property_id (str): The property ID to clone.
        version (str): The version number to clone.

    Returns:
        dict: The response from the clone operation.
    """
    endpoint = f"/papi/v1/properties/{property_id.replace('prp_', '')}/versions"
    url = urljoin(base_url, endpoint)
    params = {
        "contractId": contract_id,
        "groupId": group_id
    }
    body = {
        "createFromVersion": version
    }
    response = session.post(url, params=params, json=body)
    print(f"New property version status code: {response.status_code}")
    if response.status_code > 400:
        print(f"Response content: {response.text}")
    response.raise_for_status()
    return response.json()

if __name__ == "__main__":

    # Configuration
    property_name = "hdebeij.nl"
    origin_settings = read_json_file("origin_settings.json")

    dry_run = False  # Set to False to actually update the property
    activate_property = True  # Set to True to activate the new version after updating
    activate_network = "STAGING"  # Change to "PRODUCTION" if you want to activate in production
    activate_emails = ["example@example.com"]
    comments = f"Update CA settings via API"

    data_dir = "./data"
    os.makedirs(data_dir, exist_ok=True)

    session, base_url = get_akamai_session()

    # Find the property by name
    property_details = find_property_by_name(session, base_url, property_name)
    json.dump(property_details, open(f"{data_dir}/find_property_{property_name}.json", "w"), indent=4)

    if property_details:   
        property_id = property_details["propertyId"]
        contract_id = property_details["contractId"]
        group_id = property_details["groupId"]
        version = property_details["propertyVersion"]

        # Download the current property rules
        print(f"Found property: {property_name} with ID: {property_id}, Contract ID: {contract_id}, Group ID: {group_id}, Version: {version}")
        rules = get_property_rules(session, base_url, contract_id, group_id, property_id, version)
        json.dump(rules, open(f"{data_dir}/property_rules_{property_name}_{version}.json", "w"), indent=4)  

        # Update the property rules with the new origin settings - test only   
        iUpdated = change_property_rules(session, base_url, data_dir, rules, property_name, property_details, origin_settings)
        json.dump(rules, open(f"{data_dir}/property_rules_{property_name}_new.json", "w"), indent=4)

        if iUpdated > 0 and not dry_run:
            # It seems we have to update, first create a new version of the property
            new_version = new_property_version(session, base_url, contract_id, group_id, property_id, version=str(version))
            json.dump(new_version, open(f"{data_dir}/new_version_{property_name}.json", "w"), indent=4)
            print(f"New version response: {json.dumps(new_version, indent=4)}")

            # Grabbing the version from the version link in the response, as the version number is not directly returned in the response.
            versionLink = new_version.get("versionLink")
            if versionLink:
                print(f"New version created successfully. Version link: {versionLink}")
                version_no = versionLink.split("?")[0].split("/")[-1]
                print(f"New version number: {version_no}")
                property_details["propertyVersion"] = version_no
                #iUpdated = change_property_rules(session, base_url, data_dir, rules, property_name, property_details, origin_settings)
                rules["comments"] = comments

                # Now update the property rules for the new version
                response = update_property_rules(session, base_url, contract_id, group_id, property_id, version_no, rules)
                print(f"Updated property rules for new version: {version_no}")
                
                # And finally activate the new version if requested
                if activate_property:
                    activation_response = activate_property_version(session, base_url, contract_id, group_id, property_id, version_no, activate_network, activate_emails, comments)
                    json.dump(activation_response, open(f"{data_dir}/activation_response_{property_name}.json", "w"), indent=4)
                    print(f"Property {property_name} version {version_no} is being activated on {activate_network}.")
        else:
            print(f"No changeds, no behaviors updated for property: {property_name}. Dry run: {dry_run}")

    else:
        print(f"No property found with name: {property_name} (or you don't have access to it)")