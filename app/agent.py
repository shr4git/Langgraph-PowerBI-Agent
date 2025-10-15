import os
import json
from typing import Literal
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage, BaseMessage
from langchain_openai import ChatOpenAI, AzureChatOpenAI
from langgraph.graph import StateGraph, MessagesState
from langgraph.prebuilt import ToolNode  # correct import
from langchain_community.tools import tool
# NEW: Langfuse client
from langfuse import get_client

load_dotenv()
langfuse = get_client()  # reads LANGFUSE_HOST/PUBLIC/SECRET from env

# Import our PBI functions
from .tools_powerbi import list_reports, list_report_pages

def _serialize_messages(msgs: list[BaseMessage]) -> list[dict]:
    """Compact, safe serialization for logging."""
    out = []
    for m in msgs:
        role = getattr(m, "type", getattr(m, "_type", "message"))
        out.append({"role": role, "content": getattr(m, "content", "")})
    return out

# Wrap functions as LangChain tools
# Expose list_reports as a tool callable by the LLM.
# The docstring is used by tool-selection prompting to describe capability.
@tool("list_powerbi_reports")
def _list_powerbi_reports() -> str:
    """List Power BI reports in the configured workspace."""
    return json.dumps(list_reports())

# Tool callable exposed as a callable by LLM to list the pages in a report
@tool("get_powerbi_report_pages")
def _get_powerbi_report_pages(report_id: str) -> str:
    """List pages for a given Power BI report ID."""
    return json.dumps(list_report_pages(report_id))

# tools created thus far that agent uses
TOOLS = [_list_powerbi_reports, _get_powerbi_report_pages]

# use gpt-40-mini
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0).bind_tools(TOOLS)

def should_continue(state: MessagesState) -> Literal["tools", "__end__"]:
    last = state["messages"][-1]
    return "tools" if getattr(last, "tool_calls", None) else "__end__"

# ToolNode handles executing tools LLM selected
tool_node = ToolNode(TOOLS)

def call_model(state: MessagesState):
    messages = state["messages"]
    # OPTIONAL: add one-time system nudge to stop after tool results
    if messages and not any(isinstance(m, SystemMessage) for m in messages):
        messages = [SystemMessage(content="After using tools, summarize and finish.")] + messages

    # NEW: log LLM input/output to Langfuse
    with langfuse.start_as_current_span(name="agent.llm") as span:
        span.update(input={"messages": _serialize_messages(messages)})
        response = llm.invoke(messages)
        span.update(output={
            "content": response.content,
            "tool_calls": getattr(response, "tool_calls", None),
        })
    return {"messages": [response]}

# simple two-node graph
workflow = StateGraph(MessagesState)
workflow.add_node("agent", call_model)
workflow.add_node("tools", tool_node)
workflow.add_edge("__start__", "agent")
workflow.add_conditional_edges("agent", should_continue)
workflow.add_edge("tools", "agent")
app = workflow.compile()

# factory for platform entrypoint
def create_app():
    return app

# main method that was there just for testing
# kept just for testing sakes
# recommend testinf instead using run_agent.py
if __name__ == "__main__":
    user_prompt = "List my Power BI reports and their pages."
    # NEW: root span so the whole run nests together
    with langfuse.start_as_current_span(name="session.run") as root:
        root.update(input={"prompt": user_prompt})
        res = app.invoke({"messages": [HumanMessage(content=user_prompt)]})
        final_text = res["messages"][-1].content
        root.update(output={"final": final_text})
        print(final_text)

