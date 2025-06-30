import json
import os
from qdrant_client import QdrantClient, models
from sentence_transformers import SentenceTransformer

# Define paths
DATA_DIR = "data"
BIOS_FILE = os.path.join(DATA_DIR, "parsed", "parsed_bios.jsonl")

# Qdrant configuration
QDRANT_PATH = os.path.join(DATA_DIR, "qdrant_storage")
COLLECTION_NAME = "profiles"

def index_bios():
    """Reads bios, generates embeddings, and indexes them in Qdrant."""
    if not os.path.exists(BIOS_FILE):
        print(f"Error: Bios file not found at {BIOS_FILE}")
        print("Please run 'ingest/parse_bios.py' first.")
        return

    # Load the sentence transformer model
    print("Loading sentence transformer model...")
    model = SentenceTransformer('all-MiniLM-L6-v2', device='cpu')
    print("Model loaded.")

    # Initialize Qdrant client to use a local, file-based database
    os.makedirs(QDRANT_PATH, exist_ok=True)
    client = QdrantClient(path=QDRANT_PATH)
    print("Qdrant client initialized in-memory.")

    # Recreate Qdrant collection
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

    # Read bios and prepare for indexing
    points = []
    with open(BIOS_FILE, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            record = json.loads(line)
            vector = model.encode(record["bio"]).tolist()
            points.append(
                models.PointStruct(
                    id=i + 1,  # Qdrant IDs must be integers or UUIDs
                    vector=vector,
                    payload={"user_id": record["user_id"]}
                )
            )
    
    if not points:
        print("No bios found to index.")
        return

    # Upsert points to the collection
    try:
        client.upsert(
            collection_name=COLLECTION_NAME,
            points=points,
            wait=True
        )
        print(f"Successfully indexed {len(points)} bios.")
    except Exception as e:
        print(f"Failed to upsert points: {e}")

if __name__ == "__main__":
    index_bios()
