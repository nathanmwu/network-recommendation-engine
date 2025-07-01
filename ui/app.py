import streamlit as st
import re
import os
import sys
import duckdb

# Add project root to Python path to allow absolute imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from recommenders.router_agent import create_agent_executor
from retrievers.sql import get_user_details, get_user_id_by_name

# --- Constants ---
DUCKDB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'db', 'profiles.duckdb')

st.set_page_config(page_title="Network Recommendation Engine", layout="centered")

st.title("Network Recommendation Engine")

st.info("Welcome! Ask a question like \"Find users who work at Google\" or \"Find connections for u001\" to get started.")

# Initialize agent executor
if 'agent_executor' not in st.session_state:
    with st.spinner("Initializing agent..."):
        st.session_state.agent_executor = create_agent_executor()

prompt = st.text_input("Enter your prompt:", placeholder="e.g., Find users similar to u002")

if st.button("Get Recommendations"):
    if prompt:
        with st.spinner("Thinking..."):
            try:
                processed_prompt = prompt
                # Check if the prompt contains a name (e.g., "similar to Alice")
                match = re.search(r"(similar to|for|network of|connections for)\s+([A-Z][a-z] +)", prompt, re.IGNORECASE)
                
                if match:
                    name = match.group(2)
                    con = duckdb.connect(database=DUCKDB_PATH, read_only=True)
                    user_id = get_user_id_by_name(name, con)
                    con.close()

                    if user_id:
                        processed_prompt = prompt.replace(name, user_id)
                        st.info(f"Found user '{name}' (ID: {user_id}). Processing request...")
                    else:
                        st.warning(f"User '{name}' not found. Please check the name.")
                        st.stop()

                # 1. Get agent's raw output
                result = st.session_state.agent_executor.invoke({"input": processed_prompt})
                output_text = result['output']
                # st.info(f"**Agent's Response:** {output_text}")

                # 2. Parse recommendations and user IDs
                recommendations = {}
                user_ids = []

                if output_text and "Error" not in output_text and "No " not in output_text:
                    # Strategy 1: Parse 'user_id:reason' format
                    lines = output_text.strip().split('\n')
                    for line in lines:
                        parts = line.split(':', 1)
                        if len(parts) == 2:
                            user_id, reason = parts[0].strip(), parts[1].strip()
                            if re.match(r'u\d{3}', user_id):
                                recommendations[user_id] = reason
                    
                    user_ids = list(recommendations.keys())

                    # Strategy 2: If no recommendations found, parse raw user IDs from text
                    if not user_ids:
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
                            st.subheader(user['name'])
                            # Only show reason if it's available
                            if reason != "N/A":
                                st.markdown(f"**Reason:** {reason}")
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
