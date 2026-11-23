# pyscripts
Scripts, examples, mostly akamai related.

Use as example only, no warranty.

# Environment preparation
```bash
uv venv
uv pip install -r requirements.txt
```

# usage-cpcode.py

Usage of CpCodes per month with account group and reporting group information

```bash
uv run usage-cpcode.py [month] [productid]
```

Optional parameters:
- month:      format YYYY-MM, default last month
- productid:  default M-LC-169586 ("App & API Protector with Advanced Security Management - Included delivery")

Environment variables (can be in .env), defaults:
- AKAMAI_EDGERC=~/.edgerc
- AKAMAI_EDGERC_SECTION=default
- AKAMAI_ACCOUNT_SWITCH_KEY=

# akamai_reporting.py

Generates a traffic report for the previous calendar month for every contract
available to the Akamai account. The report uses the Akamai Reporting API v2
and writes and prints aggregated edge and origin hits and bytes, cache metrics,
and percentages as JSON.

Run the script with:

```bash
uv run akamai_reporting.py
```

The script writes the contract metrics to `akamai_reporting.json` in the
current directory. Use `contract_info.py` and `image_reporting.py` to render a separate overview
for each contract:

```bash
uv run contract_info.py
uv run image_reporting.py
```

The overview script reads `akamai_reporting.json` and `contract_info.json`, then
prints the traffic metrics and contract counts for each contract. It also
creates `reporting.pptx`, using `reporting_template.pptx` as the slide template
and generating one populated slide per contract. The PowerPoint count
placeholders are:

- `{{properties}}`: number of properties
- `{{hosts}}`: number of bound hostnames
- `{{protected}}`: number of protected hostnames
- `{{secconfigs}}`: number of security configurations

The current template also accepts the legacy `{{secconfig}}` spelling. Use
`--template`, `--contract-info`, or `--output` to override the default paths.
Placeholder replacement preserves formatting from the template, including text
alignment and bold run styling.

An alternate report path can be supplied as an argument:

```bash
uv run image_reporting.py path/to/akamai_reporting.json
```

`contract_info.py` requests Property Manager hostnames once for the whole
account and hostname coverage once for the whole account. Contracts without a
record in the contract information report use zero for all four counts.

For each contract, the reporting request retrieves its CP codes through the
CP Codes and Reporting Groups API and filters the Reporting API request by
those CP codes. The shared wrappers are in `akamai_cpcode.py`; they use
`/cprg/v1/cpcodes` and `/cprg/v1/reporting-groups`.

# edgehosts.py

Use the required `list` or `usage` subcommand:

```bash
uv run edgehosts.py list
uv run edgehosts.py list --contract ctr_EXAMPLE
uv run edgehosts.py usage
uv run edgehosts.py usage --contract ctr_EXAMPLE --domains example.com example.org
uv run edgehosts.py usage --dumpjson
```

`list` prints all edgehosts as JSON, optionally filtered by contract, without
performing usage or DNS checks.

`usage` retrieves current edgehost, Property Manager hostname, and Edge DNS
data, resolves CNAME chains, and prints the usage report, including unused
edgehosts and active hostnames without DNS. Data is passed directly to the
report in memory; no files are read or written by default. Add `--dumpjson` to
write `hostnames.json`, `edgehosts_map.json`, and `hosts_map.json` in the
current directory, replacing previous exports.
Both subcommands accept `--contract`; `usage` also accepts
`--domains` to restrict the domain checks. Running without a subcommand shows
a usage error instead of reading previously exported reports.

# akamai_appsec.py

Provides shared wrappers for the Application Security API calls used by
`clientlists.py`:

- `get_security_configurations`
- `get_config_version_details`
- `get_hostname_coverage`

`get_hostname_coverage` calls `/appsec/v1/hostname-coverage` and returns the
API's `hostnameCoverage` records. Pass `account_switch_key` when querying a
different account; otherwise the account switch key configured by
`akamai_client.py` is used.

All AppSec wrappers retry a `429` response after waiting 61 seconds.

# contract_info.py

Collects contract-level information from Property Manager and Application
Security:

- property IDs used by the contract
- hostnames bound to the contract
- protected hostnames
- security configurations protecting those hostnames

Run it with:

```bash
uv run contract_info.py
```

The report is written to `contract_info.json`. Property Manager hostnames are
requested once for the whole account and grouped by the `contractId` returned
in each hostname record. Hostname coverage is also requested once for the
whole account.

Authentication is initialized through `akamai_client.py` using the `default`
section in `~/.edgerc`. Set `AKAMAI_ACCOUNT_KEY` to override the
`account_key` from that section when an account switch key is required.

# update_origin_ca.py

Updates the origin certificate authority settings for all customer-origin
behaviors in an Akamai property. The script finds the active property version,
downloads its rules, creates a new property version, updates the rules, and
activates the new version on staging.

The script requires an Akamai `.edgerc` file with credentials in the `default`
section and a property name as a command-line argument:

```bash
uv run update_origin_ca.py hdebeij.nl
```

Use `--dry-run` to inspect the updated rule files without creating a new
property version, creating a new version, or activating the property:

```bash
uv run update_origin_ca.py hdebeij.nl --dry-run
```

Origin settings are loaded from `origin_settings.json`. Copy the
required values from a property with the desired CA configuration. The file
contains these fields:

```json
{
	"originCertsToHonor": "COMBO",
	"standardCertificateAuthorities": [
		"akamai-permissive",
		"THIRD_PARTY_AMAZON"
	],
	"customCertificateAuthorities": []
}
```
For a complete sample, see [origin_settings_example.json](origin_settings_example.json).

Best way to create the origin_settings.json is by grabbing it from an existing property with the right CA settings in place.
In property manager, navigate to the property, go to the "View JSON" section in the "Property Configuration Settings" 
and copy the necessary option fields to a file called origin_settings.json.

The script stores the property search result, original and updated rules, new
version response, and activation response in the `data/` directory. Review the
generated rule files before using this example against production. To activate
on production, change `activate_network` in `update_origin_ca.py` from
`"STAGING"` to `"PRODUCTION"`.

# cli_cases.py

`cli_cases.py` provides three commands, while `akamai_cases.py` contains
reusable API wrappers and case-export helpers.

`report` lists cases for one or more Akamai accounts. By default, it retrieves
active and closed cases; `--type ALL_CASES` selects that same combined set
explicitly. Results from all requested accounts are merged into one JSON file
or the Open and Closed worksheets in the Excel template:

```bash
uv run cli_cases.py report A-CCT1234 A-CCT5678 --type ALL_CASES --output cases.xlsx
```

For an alternate Excel template, pass `--template` with the report command:

```bash
uv run cli_cases.py report A-CCT1234 A-CCT5678 --output cases.xlsx --template custom_template.xlsx
```

`case` retrieves the case, comments, and attachment metadata as JSON. Output
can be printed to the terminal or written to a `.json` file:

```bash
uv run cli_cases.py case F-CS-000000 --output case.json
```

`attachment` downloads a specific attachment for a case:

```bash
uv run cli_cases.py attachment F-CS-000000 ATTACHMENTID --output attachment.pdf
```