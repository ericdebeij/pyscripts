# pyscripts
Scripts, examples, mostly akamai related

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