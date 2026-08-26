# akamai_client.py
import os
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
    
    base_url = f"https://{edgerc.get(section, 'host')}"
    
    session = requests.Session()
    session.auth = EdgeGridAuth.from_edgerc(edgerc, section)
    
    return session, base_url