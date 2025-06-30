import os
import duckdb
from neo4j import GraphDatabase
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

# --- Configuration ---
# DuckDB configuration
DUCKDB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'db', 'profiles.duckdb')

# Neo4j configuration
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

class Neo4jGraphBuilder:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def run_query(self, query, parameters=None):
        with self.driver.session() as session:
            result = session.run(query, parameters)
            return [record for record in result]

    def clear_graph(self):
        print("Clearing existing graph data...")
        # Using DETACH DELETE to remove nodes and their relationships
        self.run_query("MATCH (n) DETACH DELETE n")
        print("Graph cleared.")

    def build_graph(self, users_df):
        print(f"Building graph for {len(users_df)} users...")
        for _, row in users_df.iterrows():
            # Create User node
            self.run_query("MERGE (u:User {user_id: $user_id, name: $name})", 
                           {'user_id': row['user_id'], 'name': row['name']})

            # Create and connect School node
            if 'school' in row and pd.notna(row['school']):
                self.run_query("""
                MATCH (u:User {user_id: $user_id})
                MERGE (s:School {name: $school_name})
                MERGE (u)-[:ATTENDED]->(s)
                """, {'user_id': row['user_id'], 'school_name': row['school']})

            # Create and connect Company node
            if 'company' in row and pd.notna(row['company']):
                self.run_query("""
                MATCH (u:User {user_id: $user_id})
                MERGE (c:Company {name: $company_name})
                MERGE (u)-[:WORKED_AT]->(c)
                """, {'user_id': row['user_id'], 'company_name': row['company']})
        print("Graph building complete.")

def main():
    """Main function to orchestrate graph building."""
    # 1. Connect to DuckDB and fetch user data
    print(f"Connecting to DuckDB at {DUCKDB_PATH}...")
    con = duckdb.connect(database=DUCKDB_PATH, read_only=True)
    users_df = con.execute("SELECT * FROM users").fetchdf()
    con.close()
    print(f"Loaded {len(users_df)} users from DuckDB.")

    # 2. Connect to Neo4j and build the graph
    try:
        graph_builder = Neo4jGraphBuilder(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        print("Successfully connected to Neo4j.")
        
        # Clear and build
        graph_builder.clear_graph()
        graph_builder.build_graph(users_df)

    except Exception as e:
        print(f"Failed to connect or build graph in Neo4j: {e}")
        print("Please ensure Neo4j is running and credentials are correct.")
    finally:
        if 'graph_builder' in locals() and graph_builder.driver:
            graph_builder.close()
            print("Neo4j connection closed.")

if __name__ == "__main__":
    main()
