import streamlit as st
import os
import sys
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, AIMessage

# --- Setup System Path ---
# This finds the project root by going up three directories and adds it to the path
# This finds the project root by going up three directories and adds it to the path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.append(project_root)

# --- Project-specific Imports ---
try:
    from app.services.agent_service import DocumentAgent
except ImportError as e:
    st.error(f"Error: Failed to import a required module. Ensure all components exist and names are correct.")
    st.error(f"Details: {e}")
    st.stop()

# --- Load Environment ---
load_dotenv()

# --- Agent Setup ---
@st.cache_resource
def get_agent():
    """
    Initializes and returns a cached instance of the DocumentAgent.
    """
    return DocumentAgent()


# --- Streamlit UI ---
st.set_page_config(page_title="Document Assistant", page_icon="📄")
st.title("📄 Document Assistant")

# Initialize chat history for display purposes only
if "messages" not in st.session_state:
    st.session_state.messages = []
    st.session_state.messages.append({"role": "assistant", "content": "Hello! I'm the Document Assistant. How can I help you today?"})

# Display chat messages from history
for i, message in enumerate(st.session_state.messages):
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        # For the most recent assistant message that used a tool, display the context
        if message["role"] == "assistant" and i == len(st.session_state.messages) - 1 and "tool_work" in st.session_state:
            with st.expander("View Agent Work"):
                st.markdown(st.session_state.tool_work)

# Get the Agent
try:
    agent = get_agent()
except Exception as e:
    st.error("Failed to load the chatbot agent. Please try again later.")
    st.exception(e)
    st.stop()


# React to user input
if prompt := st.chat_input("Ask me anything about the documents..."):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    # Display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)

    # Generate and store assistant response
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                # The agent is now stateless; it does not receive chat history.
                response = agent.generate_response(prompt)
                
                assistant_response = response.get('output', "I seem to be at a loss for words.")
                print(f'assistant_response: {assistant_response}')
                st.markdown(assistant_response)
                
                # Store the full message dict for re-rendering
                st.session_state.messages.append({"role": "assistant", "content": assistant_response})

                # Store the agent's tool usage for the expander
                tool_work_str = ""
                if 'intermediate_steps' in response and response['intermediate_steps']:
                    for action, observation in response['intermediate_steps']:
                        tool_work_str += f"**Tool Used:** `{action.tool}`\n"
                        tool_work_str += f"**Tool Input:**\n```\n{action.tool_input}\n```\n"
                        tool_work_str += f"**Observation:**\n```\n{observation}\n```\n---\n"
                    st.session_state.tool_work = tool_work_str
                elif "tool_work" in st.session_state:
                     del st.session_state["tool_work"] # Clear it if no tool was used

            except Exception as e:
                error_message = "Sorry, I encountered an error while processing your request. Please try again."
                st.error(error_message)
                st.exception(e)
                st.session_state.messages.append({"role": "assistant", "content": error_message})

    # No full rerun needed, as we've already displayed the latest messages.
    # We just need to manage the session state correctly.
    # The loop at the start will handle redrawing on the next interaction.