import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()
import argparse

# --- Configuration ---
# --- Configuration ---
# Neo4j configuration
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

def get_graph_recommendations(user_id: str, neo4j_driver):
    """
    Finds 2nd-degree connections for a given user_id in the Neo4j graph.
    These are users connected through a shared school or company.
    Returns a list of dictionaries with user_id and the reason for the recommendation.
    """
    query = """
    MATCH (target:User {user_id: $user_id})-[:ATTENDED|WORKED_AT]->(shared_node)<-[:ATTENDED|WORKED_AT]-(recommended:User)
    WHERE target <> recommended
    WITH recommended, COLLECT(DISTINCT {type: labels(shared_node)[0], name: shared_node.name}) AS reasons
    RETURN recommended.user_id AS user_id, reasons
    LIMIT 10
    """
    with neo4j_driver.session() as session:
        result = session.run(query, user_id=user_id)
        recommendations = []
        for record in result:
            reasons_list = record["reasons"]
            # Format the reason string from the collected reasons
            reason_str = ", ".join([f"Shared {r['type']}: {r['name']}" for r in reasons_list])
            recommendations.append({
                "user_id": record["user_id"],
                "reason": reason_str
            })
        return recommendations

def main():
    """Main function to test the graph retriever."""
    parser = argparse.ArgumentParser(description="Get graph-based recommendations for a user.")
    parser.add_argument("user_id", type=str, help="The user ID to get recommendations for (e.g., 'user1').")
    args = parser.parse_args()

    driver = None
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        print(f"Successfully connected to Neo4j.")
        
        recommendations = get_graph_recommendations(args.user_id, driver)
        
        print(f"\n--- Graph Recommendations for {args.user_id} ---")
        if recommendations:
            for rec in recommendations:
                print(f"- {rec}")
        else:
            print("No recommendations found.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if driver:
            driver.close()
            print("\nNeo4j connection closed.")

if __name__ == "__main__":
    main()
