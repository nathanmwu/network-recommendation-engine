# AI Network Recommendation Engine

This project is a lightweight AI system that recommends people users may know based on shared institutions, activities, and social graphs. It features a **LangChain agent** that uses Google's Gemini model to intelligently route user queries to the best retrieval tool—SQL, vector, or graph—based on a natural language prompt.

## Repository Structure

```
network-recommendation-engine/
├── .env                # Stores environment variables (API keys, DB credentials)
├── data/
│   ├── db/             # Stores DuckDB database files
│   ├── parsed/         # Stores parsed text from unstructured data
│   ├── structured/     # Contains raw structured data (CSVs)
│   └── unstructured/   # Contains raw unstructured data (PDFs, TXTs)
├── ingest/
│   ├── load_profiles.py  # Ingests structured data into DuckDB
│   └── parse_bios.py     # Parses unstructured bios into a JSONL file
├── recommenders/
│   ├── router_agent.py     # LangChain agent for intelligent query routing
│   └── semantic_indexer.py # Creates vector embeddings for semantic search
├── retrievers/
│   ├── graph_builder.py    # Builds the Neo4j knowledge graph
│   ├── graph.py        # Retrieves recommendations from the Neo4j graph
│   ├── sql.py          # Retrieves users based on structured data from DuckDB
│   └── vector.py       # Retrieves semantically similar users from Qdrant
├── venv/                 # Python virtual environment
└── requirements.txt      # Project dependencies
```

## Setup & Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd network-recommendation-engine
    ```

2.  **Create and activate a Python virtual environment:**
    *This project requires Python 3.12.*
    ```bash
    python3.12 -m venv venv
    source venv/bin/activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Set up environment variables:**
    Create a `.env` file in the project root and add your credentials. You will need a free [Neo4j Aura](https://neo4j.com/cloud/aura/) instance and a [Google AI Studio API Key](https://aistudio.google.com/app/apikey).
    ```
    # .env
    NEO4J_URI="neo4j+s://<your-neo4j-uri>"
    NEO4J_USER="neo4j"
    NEO4J_PASSWORD="<your-neo4j-password>"
    GOOGLE_API_KEY="<your-google-api-key>"
    ```

5.  **Run the data ingestion and indexing pipelines:**
    ```bash
    # Ingest structured data
    ./venv/bin/python ingest/load_profiles.py

    # Parse unstructured data
    ./venv/bin/python ingest/parse_bios.py

    # Build vector index
    ./venv/bin/python recommenders/semantic_indexer.py

    # Build knowledge graph
    ./venv/bin/python retrievers/graph_builder.py
    ```

## How to Run

To get recommendations, run the `router_agent.py` script with a natural language prompt. The agent will interpret your query and select the best tool to answer it.

**Examples:**

*   **SQL Query:** Find users based on structured data.
    ```bash
    ./venv/bin/python recommenders/router_agent.py "Find users who work at Google"
    ```

*   **Vector Query:** Find users with similar profiles.
    ```bash
    ./venv/bin/python recommenders/router_agent.py "Find users similar to u001"
    ```

*   **Graph Query:** Find users based on network connections.
    ```bash
    ./venv/bin/python recommenders/router_agent.py "Find connections for u001"
    ```

## Core Technologies & Tools

-   **AI & Machine Learning**:
    -   **LangChain**: Framework for building LLM-powered applications.
    -   **Google Gemini**: The language model used by the agent for query routing.
    -   `sentence-transformers`: For generating text embeddings.
-   **Databases**:
    -   **Neo4j**: Graph database for storing and querying network relationships.
    -   **Qdrant**: Vector database for efficient semantic similarity search.
    -   **DuckDB**: Embedded SQL database for handling structured data.
-   **Python Libraries**:
    -   `langchain-google-genai`: Google Generative AI integration for LangChain.
    -   `neo4j`: Official Python driver for Neo4j.
    -   `qdrant-client`: Client for interacting with Qdrant.
    -   `duckdb`: Python API for DuckDB.
    -   `pandas`: For data manipulation and analysis.
    -   `PyMuPDF`: For extracting text from PDF files.
    -   `python-dotenv`: For managing environment variables.
