# Akamai API & EdgeGrid Guidelines

- Use the shared `get_akamai_session` helper from `akamai_client.py` for Akamai API
  operations:

  ```python
  from akamai_client import get_akamai_session

  session, base_url = get_akamai_session()
  ```

- Pass the returned `session` and `base_url` into API helper functions. Do not
  duplicate `EdgeRc`, `EdgeGridAuth`, or `requests.Session` initialization in
  individual scripts.
- `akamai_client.py` uses the official `akamai-edgegrid` library,
  `EdgeRc`, and `EdgeGridAuth.from_edgerc(...)`. Do not implement custom HMAC
  signing or standard bearer tokens for Akamai calls.
- Credentials are read from `~/.edgerc`, using the `default` section unless a
  different path or section is passed to `get_akamai_session`. Never commit or
  print credential contents.
- `AKAMAI_ACCOUNT_KEY`, when set, takes precedence over the `account_key` in
  `.edgerc` and is sent as the account switch key.
- Construct target URLs with
  `urllib.parse.urljoin(base_url, endpoint_path)`. Let the authenticated
  session add its configured parameters; do not build authentication headers
  manually.

# Python workspace guidelines

- The project requires Python 3.14 or newer (`pyproject.toml`).
- Use `uv` from the repository root rather than the system interpreter:

  ```bash
  uv run akamai_reporting.py
  uv run python -m py_compile akamai_reporting.py
  ```

- `python` may not be available as a shell command, and the global `python3`
  interpreter may not have project dependencies such as `requests`. Prefer
  `uv run` so the project environment and declared dependencies are used.
- To create the environment explicitly, use:

  ```bash
  uv venv
  uv pip install -r requirements.txt
  ```
