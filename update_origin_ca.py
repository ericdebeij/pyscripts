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
from akamai_papi import (
    activate_property_version,
    find_behavior,
    find_property_by_name,
    get_property_rules,
    new_property_version,
    update_property_rules,
)


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

if __name__ == "__main__":

    # Configuration
    property_name = "hdebeij.nl"
    with open("origin_settings_example.json", encoding="utf-8") as file:
        origin_settings = json.load(file)

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