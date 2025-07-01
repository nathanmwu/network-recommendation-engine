import os
import json
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer
import argparse

# --- Configuration ---
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
QDRANT_PATH = os.path.join(DATA_DIR, 'qdrant_db')
BIOS_FILE_PATH = os.path.join(DATA_DIR, 'parsed', 'parsed_bios.jsonl')
COLLECTION_NAME = "profiles"

def get_user_bio(user_id: str):
    """Retrieves the bio for a given user_id from the JSONL file."""
    try:
        with open(BIOS_FILE_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                record = json.loads(line)
                if record['user_id'] == user_id:
                    return record['bio']
    except FileNotFoundError:
        print(f"Error: Parsed bios file not found at {BIOS_FILE_PATH}")
        return None
    return None

def get_semantic_recommendations(user_id: str, qdrant_client: QdrantClient, model: SentenceTransformer):
    """Finds semantically similar users from the Qdrant index."""
    target_bio = get_user_bio(user_id)
    if not target_bio:
        print(f"Could not find bio for user {user_id}")
        return []

    # Generate embedding for the bio
    query_vector = model.encode(target_bio).tolist()

    # Search for similar vectors in Qdrant (top 5)
    search_result = qdrant_client.search(
        collection_name=COLLECTION_NAME,
        query_vector=query_vector,
        limit=5, # Return top 5, including the user themselves
    )
    
    # Extract user_ids from payload, excluding the original user
    recommendations = [
        hit.payload['user_id'] for hit in search_result 
        if hit.payload['user_id'] != user_id
    ]
    return recommendations

def main():
    """Main function to test the vector retriever."""
    parser = argparse.ArgumentParser(description="Get vector-based recommendations for a user.")
    parser.add_argument("user_id", type=str, help="The user ID to get recommendations for (e.g., 'user1').")
    args = parser.parse_args()

    try:
        qdrant_client = QdrantClient(path=QDRANT_PATH)
        model = SentenceTransformer('all-MiniLM-L6-v2', device='cpu')
        print("Successfully loaded model and connected to Qdrant.")

        recommendations = get_semantic_recommendations(args.user_id, qdrant_client, model)

        print(f"\n--- Semantic Recommendations for {args.user_id} ---")
        if recommendations:
            for rec in recommendations:
                print(f"- {rec}")
        else:
            print("No recommendations found.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
