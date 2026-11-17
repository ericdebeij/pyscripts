# akamai_client.py
import os
import sys
from urllib.parse import urljoin
import requests
from akamai.edgegrid import EdgeGridAuth, EdgeRc

def get_akamai_session(edgerc_path: str = "~/.edgerc", section: str = "default") -> tuple[requests.Session, str]:
    """
    Initializes an Akamai EdgeGrid authenticated requests Session.
    Returns:
        tuple[requests.Session, str]: Authenticated session and base URL.
    """
    expanded_path = os.path.expanduser(edgerc_path)
    edgerc = EdgeRc(expanded_path)
    account_key = os.getenv('AKAMAI_ACCOUNT_KEY')
    if not account_key:
        account_key = edgerc.get(section, 'account_key', fallback=None)  

    base_url = f"https://{edgerc.get(section, 'host')}"
    
    session = requests.Session()
    session.auth = EdgeGridAuth.from_edgerc(edgerc, section)

    if account_key:
        session.params['accountSwitchKey'] = account_key

    return session, base_url

def raise_for_status(response: requests.Response) -> None:
    if response.status_code >= 400:
        print(f"Edge response error on url: {response.url}\n status code: {response.status_code}\n body: {response.text}", file=sys.stderr)
    response.raise_for_status()
    