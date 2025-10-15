import os
import json
from typing import Literal

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage, BaseMessage
from langchain_openai import ChatOpenAI, AzureChatOpenAI
from langgraph.graph import StateGraph, MessagesState
from langgraph.prebuilt import ToolNode  # correct import
from langchain_community.tools import tool

# Keep your original Langfuse import/usage as-is
from langfuse import get_client

load_dotenv()
langfuse = get_client()  # reads LANGFUSE_HOST/PUBLIC/SECRET from env

# Import our PBI functions (adjusted to package path)
from app.tools_powerbi import list_reports, list_report_pages


def _serialize_messages(msgs: list[BaseMessage]) -> list[dict]:
    """Compact, safe serialization for logging."""
    out = []
    for m in msgs:
        role = getattr(m, "type", getattr(m, "_type", "message"))
        out.append({"role": role, "content": getattr(m, "content", "")})
    return out


# Wrap functions as LangChain tools (same behavior as your original)
@tool
def pbi_list_reports(workspace_id: str | None = None) -> str:
    """List Power BI reports in the given workspace. If no workspace_id is provided, defaults to PBI_WORKSPACE_ID env var."""
    data = list_reports(workspace_id=workspace_id)
    return json.dumps(data)


@tool
def pbi_list_report_pages(report_id: str) -> str:
    """List pages for a Power BI report by report_id."""
    data = list_report_pages(report_id=report_id)
    return json.dumps(data)


def _build_app():
    tools = [pbi_list_reports, pbi_list_report_pages]

    # Preserve your OpenAI/Azure selection pattern if present; default to OpenAI
    use_azure = os.environ.get("OPENAI_API_TYPE", "").lower() == "azure"
    if use_azure:
        llm = AzureChatOpenAI(
            azure_deployment=os.environ.get("AZURE_OPENAI_DEPLOYMENT", ""),
            api_version=os.environ.get("AZURE_OPENAI_API_VERSION", "2024-06-01"),
        ).bind_tools(tools)
    else:
        llm = ChatOpenAI(model=os.environ.get("OPENAI_MODEL", "gpt-4o-mini")).bind_tools(tools)

    def call_agent(state: MessagesState) -> MessagesState:
        result = llm.invoke(state["messages"])
        return {"messages": state["messages"] + [result]}

    def should_continue(state: MessagesState) -> str:
        last = state["messages"][-1]
        has_calls = getattr(last, "tool_calls", None)
        return "tools" if has_calls else "end"

    graph = StateGraph(MessagesState)
    graph.add_node("agent", call_agent)
    graph.add_node("tools", ToolNode(tools=tools))
    graph.add_edge("tools", "agent")
    graph.set_entry_point("agent")
    graph.add_conditional_edges("agent", should_continue, {"tools": "tools", "end": "__end__"})
    return graph.compile()


# Factory for LangGraph Platform discovery (no behavior change)
def create_app():
    return _build_app()


if __name__ == "__main__":
    app = create_app()
    out = app.invoke({"messages": [HumanMessage(content="List my Power BI reports.")]})
    print(out)

