import sys
from langchain_core.messages import HumanMessage
from .agent import app

if __name__ == "__main__":
    prompt = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "List Power BI reports"
    res = app.invoke({"messages": [HumanMessage(content=prompt)]})
    print(res["messages"][-1].content)

