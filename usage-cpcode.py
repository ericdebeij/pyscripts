"""Usage of CpCodes per month with account group and reporting group information
Optional parameters:
  month: YYYY-MM (default last month)
  productId:     (default M-LC-169586 = "App & API Protector with Advanced Security Management - Included delivery")
                 Easy way to find the productId is to look at the usage in billing of Akamai Control Center
                 and look at the URL, it contains the productId
Requires python packages: requests, akamai-edgegrid, python-dotenv
"""
import requests
import json
import sys
import csv
import datetime
import os
import dotenv
from dataclasses import dataclass
from akamai.edgegrid import EdgeGridAuth, EdgeRc
from urllib.parse import urljoin

# Load Akamai Edgegrid
dotenv.load_dotenv()
@dataclass
class Config:
    edgerc_path: str = os.path.expanduser(os.getenv('AKAMAI_EDGERC', '~/.edgerc'))
    edgerc_section: str = os.getenv('AKAMAI_EDGERC_SECTION', 'default')
    account_switch_key: str = os.getenv('AKAMAI_ACCOUNT_SWITCH_KEY', '')
cfg = Config()
edgerc = EdgeRc(cfg.edgerc_path)
baseurl = 'https://%s' % edgerc.get(cfg.edgerc_section, 'host')
sess = requests.Session()
sess.auth = EdgeGridAuth.from_edgerc(edgerc, cfg.edgerc_section)
account = cfg.account_switch_key

#Helper functions
def akurl(p):
    """Helper function to build an Akamai API string"""
    x = urljoin(baseurl, p)
    if account:
        x += "&" if "?" in x else "?"
        x += "accountSwitchKey=" + account
    return x

def checkresponse(r):
    """Helper function to check the response of an API request"""      
    if r.status_code >= 300:
        print(f"Status code: {r.status_code}", file=sys.stderr)
        print(f"{r.request.method} {r.request.url}", file=sys.stderr)
        print(f"Response:", file=sys.stderr)
        print(r.text, file=sys.stderr)
        sys.exit(1)

def isodate(dt: datetime.datetime) -> str:
    """Return datetime in ISO 8601 format with Zulu time (UTC)."""
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')

def month_add(m:str, n:int) -> str:
    """Add n months to a YYYY-MM string"""
    y, mm = m.split('-')
    y = int(y)
    mm = int(mm)
    mm += n
    while mm > 12:
        mm -= 12
        y += 1
    while mm < 1:
        mm += 12
        y -= 1
    return f"{y:04}-{mm:02}"

#Akamai API functions and manipulation
def listContracts():
    """List contracts"""
    result = sess.get(akurl('/papi/v1/contracts'))
    checkresponse(result)

    contracts = result.json()
    return contracts

def listCpCodes():
    """List all CpCodes of the account"""
    #this api does not provide groupid, well it does provide groupid but it is always nil
    result = sess.get(akurl('/cprg/v1/cpcodes'))
    checkresponse(result)

    cpcodes = result.json()["cpcodes"]
    return cpcodes

def listCpCodesOfGroup(contractId, groupId):
    """Get all CPCodes of a specific group"""
    headers = {
        "accept": "application/json",
        "PAPI-Use-Prefixes": "false"
    }
    result = sess.get(akurl(f'/papi/v1/cpcodes?contractId={contractId}&groupId={groupId}'),headers=headers)
    checkresponse(result)
    cpcodes = result.json()["cpcodes"]["items"]
    return cpcodes

def listRepGroups():
    """List reporting groups"""
    result = sess.get(akurl('/cprg/v1/reporting-groups'))
    checkresponse(result)
    repgrp = result.json()["groups"]
    return repgrp

def createMapCpcodeRepGroup():
    """Create a map of cpcode to reporting group"""
    repgroups = listRepGroups()
    repgroupMap = {}
    for rg in repgroups:
        for contract in rg["contracts"]:
            for cpcode in contract["cpcodes"]:
                cpcodeId = cpcode["cpcodeId"]
                if cpcodeId not in repgroupMap:
                    repgroupMap[cpcodeId] = []
                repgroupMap[cpcodeId].append(rg)
    return repgroupMap

def rootContract(group, groups):
    if "parentGroupId" in group:
        for x in groups:
            if x["groupId"] == group["parentGroupId"]:
                return rootContract(x, groups)

    for contractId in group["contractIds"]:
        x = contractId[4:]
        if x in group["groupName"]:
            return contractId
    return None

def groupPath(groupId, groups):
    for g in groups:
        if g["groupId"] == groupId:
            if "parentGroupId" in g:
                return groupPath(g["parentGroupId"], groups) + [g["groupName"]]
            else:
                return [g["groupName"]]
    return [] 

def listAccountGroups():
    """List policies using a specific prefix"""
    result = sess.get(akurl('/papi/v1/groups'))    
    checkresponse(result)   

    groups = result.json()["groups"]["items"]
    for group in groups:
        group["contractId"] = rootContract(group, groups)
        group["path"] = groupPath(group["groupId"], groups)
    return groups

def getUsageByCpCode(contractId, productId, start, end=None):
    """Get usage by CP code"""
    if end is None:
        end = month_add(start, 1)
    r = f'/billing/v1/contracts/{contractId}/products/{productId}/usage/by-cp-code/monthly-summary?start={start}&end={end}'
    result = sess.get(akurl(r))
    checkresponse(result)
    if result.status_code == 204:
        return 
    
    return result.json()

def getCpStatistics(contract, product, month):
    """Get CP stats for a specific contract, product and month"""
    stats = []
    usage = getUsageByCpCode(contract, product, month)
    if usage:

        for u in usage["usagePeriods"]:
            for stat in u["cpCodeStats"]:
                gb = None
                hits = None
                for s in stat["stats"]:
                    if s["statType"] == "Bytes":
                        gb = s["value"]
                    elif s["statType"] == "Hits":
                        hits = s["value"]

                stats.append({
                    "cpcode": stat["cpCode"],
                    "hits": hits,
                    "gb": gb
                })
    return stats

DELIVERY_PRODUCTS = ["Site_Accel::Site_Accel"]
def cptrafficPerMonth(productId:str, month:str, includeNoTraffic:bool=False):
    contracts = listContracts()
    contractMap = {}
    for cpcodeId in contracts["contracts"]["items"]:
        contractMap[cpcodeId["contractId"]] = cpcodeId

    cpcodes = listCpCodes()
    cpcodeMap = {}
    for cpcode in cpcodes:
        cpcodeMap[cpcode["cpcodeId"]] = cpcode

    # Create a map of cpcode to groups it is linked to
    mapCpcodeAccgroup = {}
    accountGroups = listAccountGroups()
    groupmap = {}
    for grp in accountGroups:
        groupId = int(grp["groupId"].replace("grp_",""))
        groupmap[groupId] = grp
        contractId = grp["contractId"].replace("ctr_", "")
        cpcodesOfGroup = listCpCodesOfGroup(contractId, grp["groupId"])    
        for cpi in cpcodesOfGroup:
            cpcodeId = int(cpi["cpcodeId"])
            if cpcodeId in mapCpcodeAccgroup:
                mapCpcodeAccgroup[cpcodeId].append(groupId)
            else:
                mapCpcodeAccgroup[cpcodeId] = [groupId]

    mapCpcodeRepgroup = createMapCpcodeRepGroup()

    traffic = []
    addedCpCodes = set()
    for contract in contractMap:
        cpstats = getCpStatistics(contract.replace("ctr_", ""), productId, month)   
        for cpstat in cpstats:
            cpcodeId = cpstat["cpcode"] 
            contractId = cpcodeMap[cpcodeId]["accessGroup"]["contractId"].replace("ctr_", "")
            # eror in api, groupId is always null
            #groupId = cpcodeMap[cpcode]["accessGroup"]["groupId"]
            groupId = mapCpcodeAccgroup[cpcodeId]
            groupPath = []
            for gid in groupId:
                #only use the first level groups
                if len(groupmap[gid]["path"]) == 2: 
                    groupPath.append(groupmap[gid]["path"][1])
                    #if you want the full path use this
                    #groupPath.append("/".join(groupmap[gid]["path"][1:]) if gid in groupmap else f'nf:{gid}')

            repgroupnames = []
            if cpcodeId in mapCpcodeRepgroup:
                repgroupnames = [r["reportingGroupName"] for r in mapCpcodeRepgroup[cpcodeId]]
    
            traffic.append(dict(contract=contractId, cpcode=cpcodeId, name=cpcodeMap[cpcodeId]["cpcodeName"], groupPath=groupPath, 
                                repGroups=repgroupnames,
                                hits=cpstat["hits"], gb=cpstat["gb"]))
        addedCpCodes.add(cpcodeId)   
    # Add cpcodes that have no traffic

    if not includeNoTraffic:
        for cpcodeId, cpcode in cpcodeMap.items():
            if cpcodeId not in addedCpCodes:
                validContract = False
                contractId = cpcode["accessGroup"]["contractId"].replace("ctr_", "")
                for contract in cpcode["contracts"]:
                    if contract["status"] == "ongoing":
                        validContract = True
                        break
                if not validContract:
                    continue

                validProduct = False
                for product in cpcode["products"]:
                    if product["productId"] in DELIVERY_PRODUCTS:
                        validProduct = True
                        break
                if not validProduct:
                    continue
                        
            
                groupId = mapCpcodeAccgroup[cpcodeId]
                groupPath = []
                for gid in groupId:
                    #only use the first level groups
                    if len(groupmap[gid]["path"]) == 2: 
                        groupPath.append(groupmap[gid]["path"][1])

                repgroupnames = []
                if cpcodeId in mapCpcodeRepgroup:
                    repgroupnames = [r["reportingGroupName"] for r in mapCpcodeRepgroup[cpcodeId]]

                traffic.append(dict(contract=contractId, cpcode=cpcodeId, name=cpcode["cpcodeName"], groupPath=groupPath,
                                    repGroups=repgroupnames))
            addedCpCodes.add(cpcodeId)
    return traffic

if __name__ == "__main__":
    """Main function to run the traffic analysis"""
    # Default: last month
    # Format: YYYY-MM
    month = month_add(datetime.datetime.now().strftime('%Y-%m'), -1)
    if len(sys.argv) > 1:
        month = sys.argv[1]

    # Default: "App & API Protector with Advanced Security Management - Included delivery"
    # Quick way to find the productId is to look at the usage in billing of Akamai Control Center
    # and look at the URL, it contains the productId
    productId = "M-LC-169586"  
    if len(sys.argv) > 2:
        productId = sys.argv[2]
    
    sumRepGroups = {}
    x = cptrafficPerMonth(productId, month)
    with open(f"traffic_{month}.csv", "w", newline='') as csvfile:
        fieldnames = ["contract", "cpcode", "name", "groupPath", "repGroups", "hits", "gb"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in x:
            frow = {**row, "groupPath": ";".join(row["groupPath"]), "repGroups": ";".join(row["repGroups"])}
            writer.writerow(frow)    

            for rg in row["repGroups"]:
                if rg not in sumRepGroups:
                    sumRepGroups[rg] = {"hits": 0, "gb": 0}
                sumRepGroups[rg]["hits"] += row.get("hits", 0)
                sumRepGroups[rg]["gb"] += row.get("gb", 0.0)
            
            if len(row["repGroups"]) == 0:
                if "#None" not in sumRepGroups:
                    sumRepGroups["#None"] = {"hits": 0, "gb": 0}
                sumRepGroups["#None"]["hits"] += row.get("hits", 0)
                sumRepGroups["#None"]["gb"] += row.get("gb", 0.0)

            if len(row["repGroups"]) > 1:
                n = len(row["repGroups"]) - 1
                if "#Multiple" not in sumRepGroups:
                    sumRepGroups["#Multiple"] = {"hits": 0, "gb": 0}
                sumRepGroups["#Multiple"]["hits"] -= row.get("hits", 0) * n
                sumRepGroups["#Multiple"]["gb"] -= row.get("gb", 0.0)  * n          


    print(f"Traffic for month {month} and product {productId} written to traffic_{month}.csv")
    print("Summary of Reporting Groups:")
    print("     Reporting Group:      MHits         GB")
    for rg in sorted(sumRepGroups.keys()):
        stats = sumRepGroups[rg]
        print(f"{rg:>20}: {stats['hits']/1_000_000:>10.2f} {stats['gb']:>10.2f}")
                
