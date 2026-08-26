# pyscripts
Scripts, mostly akamai relaated

# usage-cpcode.py
Usage: python3 usage-cpcode.py [month] [productid]

Optional parameters:
- month:      format YYYY-MM, default last month
- productid:  default M-LC-169586 ("App & API Protector with Advanced Security Management - Included delivery")

Environment variables (can be in .env), defaults:
- AKAMAI_EDGERC=~/.edgerc
- AKAMAI_EDGERC_SECTION=default
- AKAMAI_ACCOUNT_SWITCH_KEY=

## Running with uv
```bash
uv venv
uv pip install -r requirements.txt
uv run usage-cpcode.py [month] [productid]
```