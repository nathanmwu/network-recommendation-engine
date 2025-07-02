import json
import os
import pandas as pd
from qdrant_client import QdrantClient, models
from sentence_transformers import SentenceTransformer

# Define paths
DATA_DIR = "data"
BIOS_FILE = os.path.join(DATA_DIR, "parsed", "parsed_bios.jsonl")
USERS_CSV_FILE = os.path.join(DATA_DIR, "structured", "users.csv")

# Qdrant configuration
QDRANT_PATH = os.path.join(DATA_DIR, "qdrant_storage")
COLLECTION_NAME = "profiles"

def index_bios():
    """Reads bios from both structured CSV and parsed JSONL files,
    generates embeddings, and indexes them in Qdrant."""
    
    # --- 1. Collect all bios from different sources ---
    user_bios = {}

    # Source 1: Parsed bios from JSONL
    if os.path.exists(BIOS_FILE):
        with open(BIOS_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                record = json.loads(line)
                if record.get("user_id") and record.get("bio"):
                    user_bios[record["user_id"]] = record["bio"]
        print(f"Loaded {len(user_bios)} bios from {BIOS_FILE}")
    else:
        print(f"Warning: Parsed bios file not found at {BIOS_FILE}. Skipping.")

    # Source 2: Structured bios from users.csv
    if os.path.exists(USERS_CSV_FILE):
        csv_users_df = pd.read_csv(USERS_CSV_FILE)
        # Filter out rows where bio is missing
        csv_users_df = csv_users_df[csv_users_df['bio'].notna()]
        
        initial_count = len(user_bios)
        for _, row in csv_users_df.iterrows():
            user_bios[row['user_id']] = row['bio']
        print(f"Loaded or updated {len(user_bios) - initial_count} bios from {USERS_CSV_FILE}")
    else:
        print(f"Error: users.csv not found at {USERS_CSV_FILE}. Cannot proceed without a user source.")
        return

    if not user_bios:
        print("No user bios found from any source. Exiting.")
        return

    # --- 2. Setup model and Qdrant client ---
    print("Loading sentence transformer model...")
    model = SentenceTransformer('all-MiniLM-L6-v2', device='cpu')
    print("Model loaded.")

    os.makedirs(QDRANT_PATH, exist_ok=True)
    client = QdrantClient(path=QDRANT_PATH)
    print("Qdrant client initialized.")

    # --- 3. Recreate Qdrant collection ---
    vector_size = model.get_sentence_embedding_dimension()
    if client.collection_exists(collection_name=COLLECTION_NAME):
        client.delete_collection(collection_name=COLLECTION_NAME)
        print(f"Collection '{COLLECTION_NAME}' deleted.")

    try:
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=models.VectorParams(size=vector_size, distance=models.Distance.COSINE),
        )
        print(f"Collection '{COLLECTION_NAME}' created successfully.")
    except Exception as e:
        print(f"Failed to create collection: {e}")
        return

    # --- 4. Generate embeddings and prepare points ---
    points = []
    print(f"Generating embeddings for {len(user_bios)} unique user bios...")
    
    # Using a list comprehension for creating points
    points = [
        models.PointStruct(
            id=i + 1,
            vector=model.encode(bio).tolist(),
            payload={"user_id": user_id}
        )
        for i, (user_id, bio) in enumerate(user_bios.items())
    ]

    # --- 5. Upsert points to the collection ---
    try:
        client.upsert(
            collection_name=COLLECTION_NAME,
            points=points,
            wait=True
        )
        print(f"Successfully indexed {len(points)} bios into Qdrant.")
    except Exception as e:
        print(f"Failed to upsert points: {e}")

if __name__ == "__main__":
    index_bios()
