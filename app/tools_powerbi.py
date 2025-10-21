#tools_powerbi.py (Langfuse v3 compatible)
import os
import json
import csv
import io
import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
from langfuse import get_client  # v3 style
from langchain_openai import ChatOpenAI

# load variables from .env
load_dotenv()

# Microsoft Entra (Azure AD) application credentials and workspace context
TENANT_ID = os.environ["TENANT_ID"]
CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]
WORKSPACE_ID = os.environ.get("PBI_WORKSPACE_ID")

# Dataset to target for DAX executeQueries
# Keep this configurable via env with a fallback constant if you prefer
DATASET_ID = os.environ.get("DATASET_BANK_ID")

# OAuth2 token endpoint for the given tenant (v2.0 endpoint)
TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
# Scope for Power BI resource in client-credentials: use the application’s configured permissions
# .default suffix instructs AAD to issue a token with the app's tenant-consented scopes.
SCOPE = "https://analysis.windows.net/powerbi/api/.default"
# Power BI REST API base path
API_BASE = "https://api.powerbi.com/v1.0/myorg"

# Initialize Langfuse v3 client. It reads LANGFUSE_HOST, LANGFUSE_PUBLIC_KEY, LANGFUSE_SECRET_KEY from env
langfuse = get_client()

# LLM for DAX generation (same model as agent; temperature 0 to be deterministic)
_dax_llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

# Shared DAX prompt so the agent/tool behavior is consistent
DAX_SYSTEM_PROMPT = """Convert the user's question into a single valid DAX Query View statement for this model.

Constraints:
- Use only the table 'Ledger'.
- Valid columns: 'Ledger'[Customer Name], 'Ledger'[Account Number], 'Ledger'[Account Balance], 'Ledger'[Deposits], 'Ledger'[Interest].
- Output exactly one complete DAX query starting with EVALUATE.
- Output DAX only. No explanations, no comments.
"""

# ---------- auth ----------
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

# ---------- helper: compact CSV ----------
def _first_table_to_csv(result: dict, max_rows: int = 50) -> str:
    """
    Returns a compact CSV string from the first result table.
    If no tables/rows, returns the JSON result pretty-printed.
    Limits to max_rows rows to keep chat outputs small.
    """
    results = result.get("results", [])
    if not results:
        return json.dumps(result, indent=2)
    first = results[0]
    if first.get("errors"):
        return json.dumps(first["errors"], indent=2)
    tables = first.get("tables", [])
    if not tables:
        return json.dumps(first, indent=2)
    table = tables[0]
    columns = table.get("columns")
    rows = table.get("rows", [])
    output = io.StringIO()
    writer = csv.writer(output)

    # columns can be a list of column objects with .name, or absent; rows may be dicts
    if isinstance(columns, list):
        col_names = [c.get("name", "") for c in columns]
        writer.writerow(col_names)
        for i, row in enumerate(rows[:max_rows]):
            writer.writerow([row.get(cn, "") for cn in col_names])
    elif rows and isinstance(rows[0], dict):
        col_names = list(rows[0].keys())
        writer.writerow(col_names)
        for i, row in enumerate(rows[:max_rows]):
            writer.writerow([row.get(cn, "") for cn in col_names])
    else:
        # Unknown shape; just dump JSON
        return json.dumps(table, indent=2)

    return output.getvalue().strip()

def list_reports(workspace_id: str | None = None) -> list[dict]:
    ws = workspace_id or WORKSPACE_ID
    with langfuse.start_as_current_span(name="pbi.list_reports") as span:
        if not ws:
            span.update(metadata={"error": "workspace_id missing"})
            raise ValueError("workspace_id is required (set PBI_WORKSPACE_ID or pass explicitly)")
        token = _get_token()
        headers = {"Authorization": f"Bearer {token}"}
        url = f"{API_BASE}/groups/{ws}/reports"
        r = requests.get(url, headers=headers, timeout=30)
        span.update(metadata={"http": {"method": "GET", "url": url, "status": r.status_code}})
        r.raise_for_status()
        data = r.json().get("value", [])
        items = [
            {"id": d.get("id"), "name": d.get("name"), "datasetId": d.get("datasetId"), "webUrl": d.get("webUrl")}
            for d in data
        ]
        span.update(output={"count": len(items)})
        return items

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

# new: NL -> DAX generation as a callable tool
def generate_dax_from_nl(user_question: str) -> dict:
    """
    Tool: Convert a natural language question into a single DAX Query View statement (EVALUATE ...).
    Returns {"dax": "<query>"}.
    """
    from langchain_core.messages import SystemMessage, HumanMessage
    with langfuse.start_as_current_span(name="dax.generate") as span:
        msgs = [
            SystemMessage(content=DAX_SYSTEM_PROMPT),
            HumanMessage(content=f"User question:\n{user_question}\nReturn only the DAX."),
        ]
        span.update(input={"question": user_question})
        resp = _dax_llm.invoke(msgs)
        dax = (resp.content or "").strip()
        # Hard guard: must start with EVALUATE per constraints
        if not dax.upper().startswith("EVALUATE"):
            raise ValueError("Generated DAX does not start with EVALUATE.")
        span.update(output={"dax": dax})
        return {"dax": dax}

# execute DAX via REST API
def execute_dax_query(dax: str, workspace_id: str | None = None, dataset_id: str | None = None) -> dict:
    """
    Tool: Execute a DAX Query View statement against a dataset via executeQueries.
    Returns a dict with {"raw": <original API JSON>, "csv_preview": <up to 50 rows CSV>} for chat display.
    """
    ws = workspace_id or WORKSPACE_ID
    ds = dataset_id or DATASET_ID
    with langfuse.start_as_current_span(name="pbi.execute_dax") as span:
        if not ws:
            span.update(metadata={"error": "workspace_id missing"})
            raise ValueError("workspace_id is required (PBI_WORKSPACE_ID)")
        if not ds:
            span.update(metadata={"error": "dataset_id missing"})
            raise ValueError("dataset_id is required (PBI_DATASET_ID or pass explicitly)")

        token = _get_token()
        url = f"{API_BASE}/groups/{ws}/datasets/{ds}/executeQueries"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}
        span.update(input={"url": url, "dax": dax})
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=120)
        span.update(metadata={"http": {"method": "POST", "url": url, "status": r.status_code}})
        try:
            r.raise_for_status()
        except requests.HTTPError:
            # Capture first 4k of response to help debugging
            err_text = r.text[:4000]
            span.update(metadata={"error": "http_error", "response_snippet": err_text})
            raise

        result = r.json()
        csv_preview = _first_table_to_csv(result, max_rows=50)
        span.update(output={"has_results": True})
        return {"raw": result, "csv_preview": csv_preview}
