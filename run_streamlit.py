import streamlit as st
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage
from app.agent import app, _serialize_messages, langfuse

load_dotenv()

# Correct: page_icon and layout are separate kwargs
st.set_page_config(
    page_title="LangGraph + Power BI (Streamlit)",
    layout="centered",
)

st.title("LangGraph Power BI Agent (Streamlit + Langfuse)")

user_prompt = st.text_input("Enter your question:")

if st.button("Run Agent"):
    with langfuse.start_as_current_span(name="session.run") as root:
        msgs = [HumanMessage(content=user_prompt)]
        root.update(input={"messages": _serialize_messages(msgs)})

        result = app.invoke({"messages": msgs})
        final_text = result["messages"][-1].content

        root.update(output={"final": final_text})
        langfuse.flush()

    st.subheader("Agent Response:")
    st.write(final_text)

