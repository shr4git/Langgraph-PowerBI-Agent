# tools_powerbi.py (Langfuse v3 compatible)
import os
import json
import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
from langfuse import get_client  # v3 style

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
# .default suffix instructs AAD to issue a token with the app's tenant-consented scopes.
SCOPE = "https://analysis.windows.net/powerbi/api/.default"
# Power BI REST API base path
API_BASE = "https://api.powerbi.com/v1.0/myorg"

# Initialize Langfuse v3 client. It reads LANGFUSE_HOST, LANGFUSE_PUBLIC_KEY, LANGFUSE_SECRET_KEY from env
langfuse = get_client()

# acquires AAD access token using client credentials
# tag for buffer of up to 3 times to retry for auth/network matters
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def _get_token() -> str:
    resp = requests.post(
        TOKEN_URL,
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "client_credentials",
            "scope": SCOPE,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

# Tool to list reports in the targeted workspace
# Wraps the operation in a Langfuse span for observability (HTTP status, result count)
def list_reports(workspace_id: str | None = None) -> list[dict]:
    # obtain workspace id and create a span which gets sent to langfuse (if configured)
    ws = workspace_id or WORKSPACE_ID
    with langfuse.start_as_current_span(name="pbi.list_reports") as span:
        if not ws:
            span.update(metadata={"error": "workspace_id missing"})
            raise ValueError("workspace_id is required (set PBI_WORKSPACE_ID or pass explicitly)")
        token = _get_token()
        headers = {"Authorization": f"Bearer {token}"}
        # Power BI "reports in group" endpoint; groupId == workspace GUID
        url = f"{API_BASE}/groups/{ws}/reports"
        # Execute the call and log essential HTTP context into the span.
        r = requests.get(url, headers=headers, timeout=30)
        span.update(metadata={"http": {"method": "GET", "url": url, "status": r.status_code}})
        r.raise_for_status()
        data = r.json().get("value", [])
        # trimmed projection (IDs, names, dataset link, and web URL)
        items = [
            {"id": d.get("id"), "name": d.get("name"), "datasetId": d.get("datasetId"), "webUrl": d.get("webUrl")}
            for d in data
        ]
        span.update(output={"count": len(items)})
        return items

# Lists pages for a given report within a workspace
# span/telemetry pattern as list reports
def list_report_pages(report_id: str, workspace_id: str | None = None) -> list[dict]:
    ws = workspace_id or WORKSPACE_ID
    with langfuse.start_as_current_span(name="pbi.list_report_pages") as span:
        if not ws:
            span.update(metadata={"error": "workspace_id missing"})
            raise ValueError("workspace_id is required")
        token = _get_token()
        headers = {"Authorization": f"Bearer {token}"}
        url = f"{API_BASE}/groups/{ws}/reports/{report_id}/pages"
        r = requests.get(url, headers=headers, timeout=30)
        span.update(metadata={"http": {"method": "GET", "url": url, "status": r.status_code}})
        r.raise_for_status()
        data = r.json().get("value", [])
        items = [{"name": d.get("name"), "displayName": d.get("displayName")} for d in data]
        span.update(output={"count": len(items)})
        return items

