import os
import json
import duckdb
import spacy
import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
DUCKDB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'db', 'profiles.duckdb')
PARSED_BIOS_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'parsed', 'parsed_bios.jsonl')
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

class Neo4jGraphBuilder:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        try:
            self.nlp = spacy.load("en_core_web_sm")
        except OSError:
            print("Downloading spaCy model 'en_core_web_sm'... Please wait.")
            spacy.cli.download("en_core_web_sm")
            self.nlp = spacy.load("en_core_web_sm")

    def close(self):
        self.driver.close()

    def run_query(self, query, parameters=None):
        with self.driver.session() as session:
            result = session.run(query, parameters)
            return [record for record in result]

    def clear_graph(self):
        print("Clearing existing graph data...")
        self.run_query("MATCH (n) DETACH DELETE n")
        print("Graph cleared.")

    def extract_organizations(self, text):
        doc = self.nlp(text)
        return [ent.text for ent in doc.ents if ent.label_ == 'ORG']

    def build_graph(self, users_df, user_bios):
        print(f"Building graph for {len(users_df)} users...")
        # Process structured data
        for _, row in users_df.iterrows():
            self.run_query("MERGE (u:User {user_id: $user_id, name: $name})", 
                           {'user_id': row['user_id'], 'name': row['name']})
            if pd.notna(row.get('school')):
                self.run_query("""
                MATCH (u:User {user_id: $user_id})
                MERGE (s:School {name: $school_name})
                MERGE (u)-[:ATTENDED]->(s)
                """, {'user_id': row['user_id'], 'school_name': row['school']})
            if pd.notna(row.get('company')):
                self.run_query("""
                MATCH (u:User {user_id: $user_id})
                MERGE (c:Company {name: $company_name})
                MERGE (u)-[:WORKED_AT]->(c)
                """, {'user_id': row['user_id'], 'company_name': row['company']})

        # Process unstructured data
        print(f"Enriching graph with bio data for {len(user_bios)} users...")
        for user_id, bio in user_bios.items():
            organizations = self.extract_organizations(bio)
            for org in organizations:
                self.run_query("""
                MATCH (u:User {user_id: $user_id})
                MERGE (c:Company {name: $company_name})
                MERGE (u)-[:WORKED_AT]->(c)
                """, {'user_id': user_id, 'company_name': org})
        print("Graph building complete.")

def load_parsed_bios(file_path):
    user_bios = {}
    with open(file_path, 'r') as f:
        for line in f:
            data = json.loads(line)
            user_bios[data['user_id']] = data['bio']
    return user_bios

def main():
    # 1. Load data from DuckDB and JSONL
    print(f"Connecting to DuckDB at {DUCKDB_PATH}...")
    con = duckdb.connect(database=DUCKDB_PATH, read_only=True)
    users_df = con.execute("SELECT * FROM users").fetchdf()
    con.close()
    print(f"Loaded {len(users_df)} users from DuckDB.")
    
    print(f"Loading parsed bios from {PARSED_BIOS_PATH}...")
    user_bios = load_parsed_bios(PARSED_BIOS_PATH)
    print(f"Loaded bios for {len(user_bios)} users.")

    # 2. Connect to Neo4j and build the graph
    try:
        graph_builder = Neo4jGraphBuilder(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        print("Successfully connected to Neo4j.")
        
        graph_builder.clear_graph()
        graph_builder.build_graph(users_df, user_bios)

    except Exception as e:
        print(f"Failed to connect or build graph in Neo4j: {e}")
    finally:
        if 'graph_builder' in locals() and graph_builder.driver:
            graph_builder.close()
            print("Neo4j connection closed.")

if __name__ == "__main__":
    main()
