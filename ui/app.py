import streamlit as st
import duckdb
import os
import re
import spacy
import sys
import ast

# Add project root to Python path to allow absolute imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from recommenders.router_agent import create_agent_executor
from retrievers.sql import get_user_details, get_user_id_by_name

# --- Constants ---
DUCKDB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'db', 'profiles.duckdb')

st.set_page_config(page_title="Network Recommendation Engine", layout="centered")

st.title("Network Recommendation Engine")

st.info("Welcome! Ask a question like \"Find users who work at Google\" or \"Find connections for Alice Heart\" to get started.")

# Initialize agent executor
if 'agent_executor' not in st.session_state:
    with st.spinner("Initializing agent..."):
        st.session_state.agent_executor = create_agent_executor()

prompt = st.text_input("Enter your prompt:", placeholder="e.g., Find users similar to Alice Heart")

if st.button("Get Recommendations"):
    if prompt:
        with st.spinner("Thinking..."):
            try:
                # --- Name-to-ID Resolution ---
                processed_prompt = prompt
                nlp = spacy.load("en_core_web_sm")
                doc = nlp(prompt)
                name_to_find = next((ent.text for ent in doc.ents if ent.label_ == "PERSON"), None)

                if name_to_find:
                    user_id = get_user_id_by_name(name_to_find)
                    if user_id:
                        processed_prompt = prompt.replace(name_to_find, user_id)
                        st.info(f"Found user '{name_to_find}.' Searching their network...")
                    else:
                        st.warning(f"Could not find a user named '{name_to_find}'. Please try another name.")
                        st.stop()
                
                # 1. Get agent's raw output, including intermediate steps
                result = st.session_state.agent_executor.invoke({"input": processed_prompt})
                output_text = result['output']

                # 2. Parse recommendations from the raw tool output in intermediate_steps
                recommendations = {}
                user_ids = []
                
                if 'intermediate_steps' in result and result['intermediate_steps']:
                    # Iterate over all tool calls to aggregate results
                    for step in result['intermediate_steps']:
                        tool_output = step[1]  # This is the observation from the tool
                        if isinstance(tool_output, list):
                            for item in tool_output:
                                user_id = item.get('user_id')
                                reason = item.get('reason')
                                if user_id and reason:
                                    # If user is already recommended, append the new reason
                                    if user_id in recommendations:
                                        recommendations[user_id] += f" & {reason}"
                                    else:
                                        recommendations[user_id] = reason
                    user_ids = list(recommendations.keys())

                # Fallback: If intermediate steps didn't yield users, parse the final output text
                if not user_ids and output_text:
                    user_ids = list(set(re.findall(r'u\d{3}', output_text)))

                if user_ids:
                    # 3. Get user details from DuckDB
                    users = get_user_details(user_ids)

                    # 4. Display results in cards
                    st.success("Found the following users:")
                    for user in users:
                        with st.container(border=True):
                            # Get specific reason if available, otherwise "N/A"
                            reason = recommendations.get(user['user_id'], "N/A")
                            st.subheader(f"{user['name']} - *{user.get('title', 'N/A')}*")
                            # Display the reason in a distinct caption format if available
                            if reason != "N/A":
                                st.caption(f"{reason}")
                            st.write(f"**Email:** {user.get('email', 'N/A')}")
                            st.write(f"**Company:** {user.get('company', 'N/A')}")
                            st.write(f"**School:** {user.get('school', 'N/A')}")
                            st.write(f"**Location:** {user.get('location', 'N/A')}")
                            with st.expander("View Bio"):
                                st.write(user.get('bio', 'No bio available.'))
                else:
                    # Display the agent's raw output if no users are found or if there's a message
                    st.warning(output_text)

            except Exception as e:
                st.error(f"An error occurred: {e}")
    else:
        st.warning("Please enter a prompt.")
