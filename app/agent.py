import os
import json
from typing import Literal
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage, BaseMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, MessagesState
from langgraph.prebuilt import ToolNode
from langchain_community.tools import tool
from langfuse import get_client

load_dotenv()
langfuse = get_client()

# Import Power BI tools (now includes NL->DAX and execute)
from .tools_powerbi import (
    list_reports,
    list_report_pages,
    generate_dax_from_nl,
    execute_dax_query,
    DAX_SYSTEM_PROMPT,  # optional: if you want to surface in system prompts
)

def _serialize_messages(msgs: list[BaseMessage]) -> list[dict]:
    out = []
    for m in msgs:
        role = getattr(m, "type", getattr(m, "_type", "message"))
        out.append({"role": role, "content": getattr(m, "content", "")})
    return out

# Wrap functions as LangChain tools
@tool("list_powerbi_reports")
def _list_powerbi_reports() -> str:
    """List Power BI reports in the configured workspace."""
    return json.dumps(list_reports())

@tool("get_powerbi_report_pages")
def _get_powerbi_report_pages(report_id: str) -> str:
    """List pages for a given Power BI report ID."""
    return json.dumps(list_report_pages(report_id))

@tool("generate_dax_from_nl")
def _generate_dax_from_nl(user_question: str) -> str:
    """
    Convert a natural language question into a single DAX Query View statement for the 'Ledger' table.
    Returns JSON: {"dax": "<query>"}.
    """
    return json.dumps(generate_dax_from_nl(user_question))

@tool("execute_dax_query")
def _execute_dax_query(dax: str) -> str:
    """
    Execute a DAX query via Power BI REST executeQueries for the configured workspace/dataset.
    Returns JSON: {"raw": <API JSON>, "csv_preview": "<csv>"}.
    """
    return json.dumps(execute_dax_query(dax))

TOOLS = [
    _list_powerbi_reports,
    _get_powerbi_report_pages,
    _generate_dax_from_nl,
    _execute_dax_query,
]

# Model with tool calling
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0).bind_tools(TOOLS)

def should_continue(state: MessagesState) -> Literal["tools", "__end__"]:
    last = state["messages"][-1]
    return "tools" if getattr(last, "tool_calls", None) else "__end__"

tool_node = ToolNode(TOOLS)

def call_model(state: MessagesState):
    messages = state["messages"]
    # One-time system nudge: encourages summarizing final answer after tool use
    if messages and not any(isinstance(m, SystemMessage) for m in messages):
        messages = [SystemMessage(content="After using tools, summarize results and finish.")] + messages

    # Log LLM input/output to Langfuse
    with langfuse.start_as_current_span(name="agent.llm") as span:
        span.update(input={"messages": _serialize_messages(messages)})
        response = llm.invoke(messages)
        span.update(output={
            "content": response.content,
            "tool_calls": getattr(response, "tool_calls", None),
        })
    return {"messages": [response]}

workflow = StateGraph(MessagesState)
workflow.add_node("agent", call_model)
workflow.add_node("tools", tool_node)
workflow.add_edge("__start__", "agent")
workflow.add_conditional_edges("agent", should_continue)
workflow.add_edge("tools", "agent")
app = workflow.compile()

def create_app():
    return app

if __name__ == "__main__":
    # Example: ask a natural language question that should trigger NL->DAX then execute
    user_prompt = "Show top 5 customers by Account Balance from Ledger."
    with langfuse.start_as_current_span(name="session.run") as root:
        root.update(input={"prompt": user_prompt})
        res = app.invoke({"messages": [HumanMessage(content=user_prompt)]})
        final_text = res["messages"][-1].content
        root.update(output={"final": final_text})
        print(final_text)
