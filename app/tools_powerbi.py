# Your original logic, without added tracing calls

import os
import json
import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

# If you import langfuse in your original, keep it; otherwise omit.
# from langfuse import get_client

# load variables from .env
load_dotenv()

# Microsoft Entra (Azure AD) application credentials and workspace context
TENANT_ID = os.environ["TENANT_ID"]
CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]

# WORKSPACE_ID: Power BI "groupId" (GUID) identifying the target workspace.
WORKSPACE_ID = os.environ.get("PBI_WORKSPACE_ID")

# OAuth2 token endpoint for the given tenant (v2.0 endpoint)
TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

# Scope for Power BI resource in client-credentials: use the application’s configured permissions
# .default uses tenant-consented application permissions.
SCOPE = "https://analysis.windows.net/powerbi/api/.default"

# Power BI REST API base
PBI_BASE = "https://api.powerbi.com/v1.0/myorg"

# If using Langfuse originally as a client only (no .trace()):
# langfuse = get_client()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=6))
def _get_token() -> str:
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": SCOPE,
        "grant_type": "client_credentials",
    }
    resp = requests.post(TOKEN_URL, data=data, timeout=20)
    resp.raise_for_status()
    return resp.json()["access_token"]


def _auth_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def list_reports(workspace_id: str | None = None) -> dict:
    """Return a dict with reports metadata for the given workspace or default workspace."""
    token = _get_token()
    wid = workspace_id or WORKSPACE_ID
    if not wid:
        return {"error": "workspace_id is required (env PBI_WORKSPACE_ID or argument)"}

    url = f"{PBI_BASE}/groups/{wid}/reports"
    r = requests.get(url, headers=_auth_headers(token), timeout=20)
    if r.status_code != 200:
        return {"error": f"PBI API error {r.status_code}", "body": r.text}
    body = r.json()
    items = [
        {"id": it.get("id"), "name": it.get("name"), "datasetId": it.get("datasetId")}
        for it in body.get("value", [])
    ]
    return {"workspace_id": wid, "reports": items}


def list_report_pages(report_id: str) -> dict:
    """Return a dict with page metadata for a given report id."""
    token = _get_token()
    if not report_id:
        return {"error": "report_id is required"}

    url = f"{PBI_BASE}/reports/{report_id}/pages"
    r = requests.get(url, headers=_auth_headers(token), timeout=20)
    if r.status_code != 200:
        return {"error": f"PBI API error {r.status_code}", "body": r.text}
    body = r.json()
    pages = [
        {"name": it.get("name"), "displayName": it.get("displayName"), "order": it.get("order")}
        for it in body.get("value", [])
    ]
    return {"report_id": report_id, "pages": pages}

