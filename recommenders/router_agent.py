import os
import sys
import argparse
from dotenv import load_dotenv

# --- LangChain Imports ---
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.agents import tool, AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

# --- Database and Retriever Imports ---
import duckdb
from neo4j import GraphDatabase
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

# Add project root to path to allow direct script execution
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)

from retrievers.sql import get_sql_recommendations
from retrievers.vector import get_semantic_recommendations
from retrievers.graph import get_graph_recommendations

# --- Configuration ---
load_dotenv()
if not os.getenv("GOOGLE_API_KEY"):
    raise ValueError("GOOGLE_API_KEY not found in .env file. Please add it to proceed.")

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
DUCKDB_PATH = os.path.join(DATA_DIR, 'db', 'profiles.duckdb')
QDRANT_STORAGE_PATH = os.path.join(DATA_DIR, 'qdrant_storage')

# --- Tool Definitions ---

@tool
def sql_retriever(query: str) -> list:
    """Searches for users based on structured data like company, school, or location.
    Use this for queries like 'Find users who work at Google' or 'Who went to MIT?'.
    The query MUST be in the format 'field:value', e.g., 'company:Google' or 'school:Stanford'."""
    try:
        field, value = query.split(':', 1)
        field = field.strip()
        value = value.strip()
        con = duckdb.connect(database=DUCKDB_PATH, read_only=True)
        results = get_sql_recommendations(field, value, con)
        con.close()
        return results
    except Exception as e:
        return [f"Error processing SQL query: {e}. Ensure the query is in 'field:value' format."]

@tool
def vector_retriever(user_id: str) -> list:
    """Finds users with semantically similar bios or profiles.
    Use this for queries like 'Find users similar to u001' or 'Who has a profile like u001?'."""
    try:
        client = QdrantClient(path=QDRANT_STORAGE_PATH)
        model = SentenceTransformer('all-MiniLM-L6-v2', device='cpu')
        results = get_semantic_recommendations(user_id, client, model)
        return results
    except Exception as e:
        return [f"Error during vector search: {e}"]

@tool
def graph_retriever(user_id: str) -> list:
    """Finds users connected through a shared school or company in the knowledge graph (2nd-degree connections).
    Use this for queries about network connections, like 'Who is in u001's network?' or 'Find connections for u001'."""
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        results = get_graph_recommendations(user_id, driver)
        driver.close()
        return results
    except Exception as e:
        return [f"Error connecting to graph database: {e}"]

# --- Agent Setup ---

def create_agent_executor():
    """Creates and returns the LangChain agent executor."""
    tools = [sql_retriever, vector_retriever, graph_retriever]
    
    prompt_template = """
    You are an AI assistant that helps find users in a professional network.
    Use the available tools to answer the user's request. Be precise and concise.
    """
    
    prompt = ChatPromptTemplate.from_messages([
        ("system", prompt_template),
        MessagesPlaceholder(variable_name="chat_history", optional=True),
        ("human", "{input}"),
        MessagesPlaceholder(variable_name="agent_scratchpad"),
    ])

    llm = ChatGoogleGenerativeAI(model="gemini-2.0-flash", temperature=0, convert_system_message_to_human=True)
    agent = create_tool_calling_agent(llm, tools, prompt)
    agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)
    
    return agent_executor

def main():
    """Main function to run the agent from the command line."""
    parser = argparse.ArgumentParser(description="Use a LangChain agent to get recommendations.")
    parser.add_argument("prompt", type=str, help="The natural language prompt for the agent.")
    args = parser.parse_args()

    agent_executor = create_agent_executor()
    
    print(f"\n🤖 Sending prompt to agent: '{args.prompt}'")
    print("-" * 30)

    result = agent_executor.invoke({"input": args.prompt})

    print("-" * 30)
    print(f"✅ Agent Response:")
    print(result["output"])

if __name__ == "__main__":
    main()
