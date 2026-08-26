# Akamai API & EdgeGrid Guidelines

- Always use the official `akamai-edgegrid` library (`from akamai.edgegrid import EdgeGridAuth, EdgeRc`) for Akamai API operations.
- Do NOT implement custom HMAC signing or standard Bearer header tokens for Akamai calls.
- Read credentials dynamically from `~/.edgerc` using `EdgeRc`.
- Construct target URLs using `urllib.parse.urljoin(base_url, endpoint_path)`.
- Use `requests.Session()` with `session.auth = EdgeGridAuth.from_edgerc(...)` for connection pooling.